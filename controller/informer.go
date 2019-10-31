package controller

import (
	"context"
	"github.com/coreos/etcd/clientv3"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog"
	"sync"
	"time"
)

type Config struct {
	lw           ListWatch
	keyFunc      func(obj interface{}) (string, error)
	queue        cache.Queue
	processFunc  ProcessFunc
	retryOnError bool
	reflector    *Reflector
}

type KeyFuncs struct {
	KeyFunc                 func(obj interface{}) (string, error)
	DeletionHandlingKeyFunc func(obj interface{}) (string, error)
}

type ListWatch interface {
	Watch(ctx context.Context, modVersion int64) clientv3.WatchChan

	List() (*clientv3.GetResponse, error)
}

type ProcessFunc func(obj interface{}) error

func NewInformer(
	lw ListWatch,
	deserializeFunc func(data []byte) (interface{}, error),
	resyncPeriod time.Duration,
	f KeyFuncs,
	h cache.ResourceEventHandlerFuncs,
) (cache.Store, Controller) {

	clientState := cache.NewStore(f.DeletionHandlingKeyFunc)

	fifo := cache.NewDeltaFIFO(f.KeyFunc, clientState)

	cfg := &Config{
		queue:        fifo,
		retryOnError: false,
		reflector: NewReflector(
			lw,
			f,
			deserializeFunc,
			resyncPeriod,
			fifo,
		),
		processFunc: func(obj interface{}) error {
			// from oldest to newest
			for _, d := range obj.(cache.Deltas) {
				switch d.Type {
				case cache.Sync, cache.Added, cache.Updated:
					if old, exists, err := clientState.Get(d.Object); err == nil && exists {
						if err := clientState.Update(d.Object); err != nil {
							return err
						}
						h.OnUpdate(old, d.Object)
					} else {
						if err := clientState.Add(d.Object); err != nil {
							return err
						}
						h.OnAdd(d.Object)
					}
				case cache.Deleted:
					if err := clientState.Delete(d.Object); err != nil {
						return err
					}
					h.OnDelete(d.Object)
				}
			}
			return nil
		},
	}
	return clientState, New(cfg)
}

// New makes a new Controller from the given Config.
func New(c *Config) Controller {
	return &controller{
		config:    *c,
		reflector: c.reflector,
	}
}

// Controller is a generic controller framework.
type controller struct {
	config         Config
	reflector      *Reflector
	reflectorMutex sync.RWMutex
}

type Controller interface {
	Run(stopCh <-chan struct{})
	HasSynced() bool
	LastSyncResourceVersion() int64
}

// Run begins processing items, and will continue until a value is sent down stopCh.
// It's an error to call Run more than once.
// Run blocks; call via go.
func (c *controller) Run(stopCh <-chan struct{}) {
	go func() {
		<-stopCh
		c.config.queue.Close()
	}()
	c.reflector.Run(stopCh)

	wait.Until(c.processLoop, time.Second, stopCh)
}

func (c *controller) HasSynced() bool {
	return c.config.queue.HasSynced()
}

func (c *controller) LastSyncResourceVersion() int64 {
	return c.reflector.LastSyncRevision()
}

func (c *controller) processLoop() {
	for {
		obj, err := c.config.queue.Pop(cache.PopProcessFunc(c.config.processFunc))
		if err != nil {
			if err == cache.ErrFIFOClosed {
				return
			}
			if c.config.retryOnError {
				c.config.queue.AddIfNotPresent(obj)
			}
		}
	}
}

func WaitForCacheSync(stopCh <-chan struct{}, cacheSyncs ...func() bool) bool {
	err := wait.PollUntil(time.Millisecond*100,
		func() (bool, error) {
			for _, syncFunc := range cacheSyncs {
				if !syncFunc() {
					return false, nil
				}
			}
			return true, nil
		},
		stopCh)
	if err != nil {
		klog.Infof("stop requested")
		return false
	}

	klog.Infof("caches populated")
	return true
}
