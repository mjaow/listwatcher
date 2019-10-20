package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/embed"
	"github.com/stretchr/testify/assert"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

type testStudent struct {
	Id   int    `json:"id"`
	Name string `json:"name"`
}

func (t *testStudent) Key() string {
	return fmt.Sprintf("%d", t.Id)
}

func deserializeTestStudenFunc(data []byte) (interface{}, error) {
	var item testStudent

	err := json.Unmarshal(data, &item)
	if err != nil {
		return nil, err
	}

	return &item, nil
}

func testStudentKeyFuncs(obj interface{}) (s string, e error) {
	if pod, ok := obj.(*testStudent); ok {
		return pod.Key(), nil
	}
	return "", fmt.Errorf("type is not *testStudent")
}

func testStudentDeletionHandlingKeyFuncKeyFuncs(obj interface{}) (s string, e error) {
	if d, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		return d.Key, nil
	}
	return testStudentKeyFuncs(obj)
}

type testStudentListWatch struct {
	cli     *clientv3.Client
	cluster string
	timeout time.Duration
}

func NewTestStudentListWatch(cli *clientv3.Client, cluster string, timeout time.Duration) *testStudentListWatch {
	return &testStudentListWatch{
		cli:     cli,
		cluster: cluster,
		timeout: timeout,
	}
}

func (rm *testStudentListWatch) Watch(ctx context.Context, modVersion int64) clientv3.WatchChan {
	return rm.cli.Watch(
		ctx,
		fmt.Sprintf("/v3/%s/%s", "ts", rm.cluster),
		clientv3.WithRev(modVersion), clientv3.WithPrefix(), clientv3.WithPrevKV())
}

func (rm *testStudentListWatch) List() (*clientv3.GetResponse, error) {
	ctx, _ := context.WithTimeout(context.Background(), rm.timeout)
	return rm.cli.Get(ctx, fmt.Sprintf("/v3/%s/%s", "ts", rm.cluster), clientv3.WithPrefix())
}

func TestInformer(t *testing.T) {
	StartEtcdServer()

	timeout := time.Second * 3

	ctx, _ := context.WithTimeout(context.Background(), timeout)
	cli, err := clientv3.New(LocalEtcdCfg)
	if err != nil {
		t.Fatal(err)
	}
	cli.Delete(ctx, "/", clientv3.WithPrefix())

	var op []string

	var mu sync.Mutex

	cluster := "testcluster"

	store, rm := NewInformer(NewTestStudentListWatch(cli, cluster, timeout), deserializeTestStudenFunc, time.Second*3, testStudentDeletionHandlingKeyFuncKeyFuncs, testStudentKeyFuncs, cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			mu.Lock()
			defer mu.Unlock()

			if k, e := testStudentKeyFuncs(obj); e == nil {
				op = append(op, "add "+k)
			}
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			mu.Lock()
			defer mu.Unlock()
			if k, e := testStudentKeyFuncs(newObj); e == nil {
				op = append(op, "update "+k)
			}
		},
		DeleteFunc: func(obj interface{}) {
			mu.Lock()
			defer mu.Unlock()
			if k, e := testStudentDeletionHandlingKeyFuncKeyFuncs(obj); e == nil {
				op = append(op, "delete "+k)
			}
		},
	})
	if err := store.Add(&testStudent{
		Id:   3,
		Name: "jack",
	}); err != nil {
		t.Fatal(err)
	}

	stopCh := make(chan struct{})

	go rm.Run(stopCh)

	if !WaitForCacheSync(stopCh, rm.HasSynced) {
		t.Fatalf("timed out waiting for caches to sync")
		return
	}

	time.Sleep(time.Second)

	assert.EqualValues(t, []string{}, store.ListKeys())

	if _, e := cli.Put(ctx, fmt.Sprintf("/v3/%s/%s/1", "ts", cluster), `{
		"id":1,
		"name":"tom"
	}`); e != nil {
		t.Fatal(e)
	}

	if _, e := cli.Put(ctx, fmt.Sprintf("/v3/%s/%s/2", "ts", cluster), `
	{
		"id":2,
		"name":"jack"
	}
	`); e != nil {
		t.Fatal(e)
	}

	if _, e := cli.Put(ctx, fmt.Sprintf("/v3/%s/%s/3", "ts", cluster), `
	{
		"id":3,
		"name":"jerry"
	}
	`); e != nil {
		t.Fatal(e)
	}
	if _, e := cli.Put(ctx, fmt.Sprintf("/v3/%s/%s/2", "ts", cluster), `
	{
		"id":2,
		"name":"harry"
	}
	`); e != nil {
		t.Fatal(e)
	}

	if _, e := cli.Delete(ctx, fmt.Sprintf("/v3/%s/%s/2", "ts", cluster)); e != nil {
		t.Fatal(e)
	}

	time.Sleep(time.Second * 3)

	close(stopCh)

	time.Sleep(time.Second)

	mu.Lock()

	assert.EqualValues(t, []string{"delete 3", "add 1", "add 2", "add 3", "update 2", "delete 2", "update 1", "update 3"}, op)

	mu.Unlock()

	assert.EqualValues(t, []string{"1", "3"}, sortedList(store.ListKeys()))

}

func sortedList(s [] string) []string {
	sort.Strings(s)
	return s
}

var path = "/tmp/lw.etcd"

var LocalEtcdCfg = clientv3.Config{
	Endpoints:        []string{"127.0.0.1:2379"},
	AutoSyncInterval: time.Second * 3,
	DialTimeout:      time.Second * 3,
}

var etcdServer *embed.Etcd

func StartEtcdServer() {
	start := time.Now()
	for {
		time.Sleep(time.Second * 1)
		cfg := embed.NewConfig()
		cfg.Dir = path
		e, err := embed.StartEtcd(cfg)
		if err != nil {
			if time.Now().Sub(start).Seconds() > 30 {
				panic(err)
			}

			if !strings.Contains(err.Error(), "address already in use") {
				panic(err)
			}
			continue
		}
		select {
		case <-e.Server.ReadyNotify():
			klog.Infof("Server is ready!")
		}
		etcdServer = e
		return
	}
}

func StopEtcdServer() {
	etcdServer.Server.Stop()
	etcdServer.Close()
}
