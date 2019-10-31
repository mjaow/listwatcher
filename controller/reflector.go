package controller

import (
	"context"
	"errors"
	"fmt"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
)

var neverExitWatch <-chan time.Time = make(chan time.Time)

type Reflector struct {
	listFunc              func() (*clientv3.GetResponse, error)
	watchFunc             func(ctx context.Context, modVersion int64) clientv3.WatchChan
	deserializeFunc       func(data []byte) (interface{}, error)
	resyncPeriod          time.Duration
	store                 cache.Store
	lastSyncRevision      int64
	lastSyncRevisionMutex sync.RWMutex
	keyFuncs              KeyFuncs
}

func NewReflector(
	lw ListWatch,
	k KeyFuncs,
	deserializeFunc func(data []byte) (interface{}, error),
	resyncPeriod time.Duration,
	store cache.Store) *Reflector {
	return &Reflector{
		listFunc:         lw.List,
		watchFunc:        lw.Watch,
		keyFuncs:         k,
		deserializeFunc:  deserializeFunc,
		resyncPeriod:     resyncPeriod,
		store:            store,
		lastSyncRevision: 0,
	}
}

func (rm *Reflector) Run(stopCh <-chan struct{}) {
	go wait.Until(func() { rm.ListAndWatch(stopCh) }, time.Second, stopCh)
}

func (rm *Reflector) syncWith(kvs []*mvccpb.KeyValue) (int64, error) {
	var lastRevision int64

	var items []interface{}
	for _, kv := range kvs {
		rc, err := rm.deserializeFunc(kv.Value)

		if err != nil {
			return 0, fmt.Errorf("deserialize key %s value %s failed: %v", string(kv.Key), string(kv.Value), err)
		}

		if err := rm.checkKey(string(kv.Key), rc); err != nil {
			if err != KeyConflictErr {
				return 0, err
			} else {
				klog.Errorf("key conflict for key %s and data %v", string(kv.Key), rc)
				continue
			}
		}

		lastRevision = kv.ModRevision

		items = append(items, rc)
	}

	if err := rm.store.Replace(items, ""); err != nil {
		return 0, fmt.Errorf("replace items failed: %v", err)
	}

	return lastRevision + 1, nil
}

func (rm *Reflector) resyncChan() (<-chan time.Time, func()) {
	if rm.resyncPeriod <= 0 {
		return neverExitWatch, func() {}
	}
	t := time.NewTicker(rm.resyncPeriod)
	return t.C, t.Stop
}

func (rm *Reflector) ListAndWatch(stopCh <-chan struct{}) {
	resync, cleanup := rm.resyncChan()

	defer cleanup()

	resp, err := rm.listFunc()

	if err != nil {
		klog.Errorf("list controller with error %v", err)
		return
	}

	currRevision, err := rm.syncWith(resp.Kvs)

	if err != nil {
		klog.Error(err)
		return
	}

	rm.setLastSyncRevision(currRevision)

	ctx, cancel := context.WithCancel(context.Background())

	watching := rm.watchFunc(ctx, 0)

	defer cancel()

	for {
		select {
		case <-stopCh:
			return
		case <-resync:
			return
		case response, open := <-watching:
			if !open {
				return
			}

			if err := response.Err(); err != nil {
				klog.Errorf("watch controller with error %v", err)
				return
			}

			for _, event := range response.Events {
				if err := rm.watchHandler(event); err != nil {
					klog.Errorf("handle watch event error %v", err)
					return
				}
			}

		}
	}
}

func (rm *Reflector) setLastSyncRevision(lastRevision int64) {
	rm.lastSyncRevisionMutex.Lock()
	defer rm.lastSyncRevisionMutex.Unlock()

	rm.lastSyncRevision = lastRevision
}

func (rm *Reflector) LastSyncRevision() int64 {
	rm.lastSyncRevisionMutex.RLock()
	defer rm.lastSyncRevisionMutex.RUnlock()

	return rm.lastSyncRevision
}

var KeyConflictErr = errors.New("KeyConflict")

func (rm *Reflector) checkKey(key string, obj interface{}) error {
	k, err := rm.keyFuncs.KeyFunc(obj)

	if err != nil {
		return err
	}

	if k != key {
		return KeyConflictErr
	}

	return nil
}

func (rm *Reflector) watchHandler(event *clientv3.Event) error {
	if event == nil {
		return fmt.Errorf("event is nil")
	}

	if event.Type != mvccpb.PUT && event.Type != mvccpb.DELETE {
		return fmt.Errorf("unknown event %v with type %s", event, event.Type)
	}

	rm.setLastSyncRevision(event.Kv.ModRevision)

	klog.Infof("About to sync from watch: %v(%d) %v", event.Type, event.Kv.ModRevision, string(event.Kv.Key))

	if event.Type == mvccpb.DELETE {
		item, err := rm.deserializeFunc(event.PrevKv.Value)

		if err != nil {
			return err
		}

		if err := rm.store.Delete(item); err != nil {
			return err
		}
	} else {
		newData, err := rm.deserializeFunc(event.Kv.Value)

		if err != nil {
			return err
		}

		if err := rm.checkKey(string(event.Kv.Key), newData); err != nil {
			if err != KeyConflictErr {
				return err
			} else {
				klog.Errorf("key conflict for key %s and data %v", string(event.Kv.Key), newData)
				return nil
			}
		}

		if _, exits, err := rm.store.GetByKey(string(event.Kv.Key)); err != nil {
			return err
		} else if exits {
			if err := rm.store.Update(newData); err != nil {
				return err
			}
		} else {
			if err := rm.store.Add(newData); err != nil {
				return err
			}
		}
	}

	return nil

}
