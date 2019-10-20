package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"github.com/mjaow/listwatcher/controller"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog"
	"time"
)

type TODO struct {
	Id   string
	Name string
}

func (t *TODO) Key() string {
	return fmt.Sprintf("%s/%s", t.Id, t.Name)
}

func deserializeTODOFunc(data []byte) (interface{}, error) {
	var item TODO

	err := json.Unmarshal(data, &item)
	if err != nil {
		return nil, err
	}

	return &item, nil
}

func TODOKeyFuncs(obj interface{}) (s string, e error) {
	if pod, ok := obj.(*TODO); ok {
		return pod.Key(), nil
	}
	return "", fmt.Errorf("type is not *TODO")
}

func TODODeletionHandlingKeyFuncKeyFuncs(obj interface{}) (s string, e error) {
	if d, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		return d.Key, nil
	}
	return TODOKeyFuncs(obj)
}

type TODOListWatch struct {
	cli     *clientv3.Client
	timeout time.Duration
}

func NewTODOListWatch(cli *clientv3.Client, timeout time.Duration) *TODOListWatch {
	return &TODOListWatch{
		cli:     cli,
		timeout: timeout,
	}
}

func (rm *TODOListWatch) Watch(ctx context.Context, modVersion int64) clientv3.WatchChan {
	return rm.cli.Watch(
		ctx,
		fmt.Sprintf("/v3/%s", "todo"),
		clientv3.WithRev(modVersion), clientv3.WithPrefix(), clientv3.WithPrevKV())
}

func (rm *TODOListWatch) List() (*clientv3.GetResponse, error) {
	ctx, _ := context.WithTimeout(context.Background(), rm.timeout)
	return rm.cli.Get(ctx, fmt.Sprintf("/v3/%s", "todo"), clientv3.WithPrefix())
}

type TODOManager struct {
	store      cache.Store
	controller controller.Controller

	queue workqueue.RateLimitingInterface
}

func NewTODOManager(etcdCli *clientv3.Client, timeout time.Duration, resyncPeriod time.Duration) *TODOManager {
	rm := &TODOManager{}
	rm.queue = workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "todomanager")
	rm.store, rm.controller = controller.NewInformer(
		NewTODOListWatch(etcdCli, timeout),
		deserializeTODOFunc,
		resyncPeriod,
		TODODeletionHandlingKeyFuncKeyFuncs,
		TODOKeyFuncs,
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				if k, err := TODOKeyFuncs(obj); err == nil {
					rm.queue.Add(k)
				}
			},
			UpdateFunc: func(oldObj, newObj interface{}) {
				if k, err := TODOKeyFuncs(newObj); err == nil {
					rm.queue.Add(k)
				}
			},
			DeleteFunc: func(obj interface{}) {
				if k, err := TODODeletionHandlingKeyFuncKeyFuncs(obj); err == nil {
					rm.queue.Add(k)
				}
			},
		})

	return rm
}

func (rm *TODOManager) Run(workers int, stopCh <-chan struct{}) {
	defer runtime.HandleCrash()
	defer rm.queue.ShutDown()

	klog.Infof("starting todo manager")

	go rm.controller.Run(stopCh)

	if !controller.WaitForCacheSync(stopCh, rm.controller.HasSynced) {
		klog.Errorf("timed out waiting for caches to sync")
		return
	}

	for i := 0; i < workers; i++ {
		go wait.Until(rm.work, time.Second, stopCh)
	}

	<-stopCh

	klog.Infof("shutdown todo manager")
}

func (rm *TODOManager) processNextItem() bool {
	item, shutdown := rm.queue.Get()

	if shutdown {
		return false
	}

	defer rm.queue.Done(item)

	err := rm.syncTODOController(item.(string))

	rm.handleErr(err, item)

	return true
}

func (rm *TODOManager) syncTODOController(key string) error {

	//TODO get desire state and actual state by key

	//TODO compare and do something to keep balance

	return nil
}

func (rm *TODOManager) handleErr(err error, key interface{}) {
	if err == nil {
		rm.queue.Forget(key)
		return
	}

	if retry := rm.queue.NumRequeues(key); retry < 5 {
		klog.Errorf("[%d]Error syncing pod %v: %v", retry, key, err)

		rm.queue.AddRateLimited(key)
		return
	}

	rm.queue.Forget(key)

	klog.Errorf("Dropping pod %q out of the queue: %v", key, err)
}

func (rm *TODOManager) work() {
	for rm.processNextItem() {
	}
}

func main() {
	//usage of list watch library

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:        []string{"127.0.0.1:2379"},
		AutoSyncInterval: time.Second * 3,
		DialTimeout:      time.Second * 3,
	})

	if err != nil {
		klog.Fatal(err)
	}

	m := NewTODOManager(cli, time.Second*3, time.Minute*5)

	go m.Run(5, wait.NeverStop)

	select {}
}
