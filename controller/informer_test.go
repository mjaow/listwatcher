package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/stretchr/testify/assert"
	"k8s.io/client-go/tools/cache"
)

const (
	ModelType = "ts"
	Cluster   = "testcluster"
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
		fmt.Sprintf("/v3/%s/%s", ModelType, rm.cluster),
		clientv3.WithRev(modVersion), clientv3.WithPrefix(), clientv3.WithPrevKV())
}

func (rm *testStudentListWatch) List() (*clientv3.GetResponse, error) {
	ctx, _ := context.WithTimeout(context.Background(), rm.timeout)
	return rm.cli.Get(ctx, fmt.Sprintf("/v3/%s/%s", ModelType, rm.cluster), clientv3.WithPrefix())
}

func TestInformerWaitForCacheSync(t *testing.T) {
	timeout := time.Second * 3

	ctx, _ := context.WithTimeout(context.Background(), timeout)
	cli := NewOrDie(t)

	if _, err := cli.Put(ctx, fmt.Sprintf("/v3/%s/%s/1", ModelType, Cluster), `
	{
		"id":1,
		"name":"jack"
	}
	`); err != nil {
		t.Fatal(err)
	}

	if _, e := cli.Put(ctx, fmt.Sprintf("/v3/%s/%s/2", ModelType, Cluster), `
	{
		"id":2,
		"name":"jack"
	}
	`); e != nil {
		t.Fatal(e)
	}

	if _, e := cli.Put(ctx, fmt.Sprintf("/v3/%s/%s/3", ModelType, Cluster), `null`); e != nil {
		t.Fatal(e)
	}

	if _, e := cli.Put(ctx, fmt.Sprintf("/v3/%s/%s/4", ModelType, Cluster), `null`); e != nil {
		t.Fatal(e)
	}

	defer cli.Delete(ctx, "/", clientv3.WithPrefix())

	_, rm := NewInformer(
		NewTestStudentListWatch(cli, Cluster, timeout),
		deserializeTestStudenFunc,
		time.Second*3,
		KeyFuncs{
			KeyFunc:                 testStudentKeyFuncs,
			DeletionHandlingKeyFunc: testStudentDeletionHandlingKeyFuncKeyFuncs,
		}, cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
			},
			UpdateFunc: func(oldObj, newObj interface{}) {
			},
			DeleteFunc: func(obj interface{}) {
			},
		})

	stopCh := make(chan struct{})

	go rm.Run(stopCh)

	time.Sleep(time.Second)

	assert.True(t, rm.HasSynced())
}

func TestInformer(t *testing.T) {
	timeout := time.Second * 3

	ctx, _ := context.WithTimeout(context.Background(), timeout)
	cli := NewOrDie(t)

	defer cli.Delete(ctx, "/", clientv3.WithPrefix())

	var op []string

	var mu sync.Mutex

	store, rm := NewInformer(
		NewTestStudentListWatch(cli, Cluster, timeout),
		deserializeTestStudenFunc,
		time.Second*3,
		KeyFuncs{
			KeyFunc:                 testStudentKeyFuncs,
			DeletionHandlingKeyFunc: testStudentDeletionHandlingKeyFuncKeyFuncs,
		}, cache.ResourceEventHandlerFuncs{
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

	if _, e := cli.Put(ctx, fmt.Sprintf("/v3/%s/%s/1", ModelType, Cluster), `{
		"id":1,
		"name":"tom"
	}`); e != nil {
		t.Fatal(e)
	}

	if _, e := cli.Put(ctx, fmt.Sprintf("/v3/%s/%s/2", ModelType, Cluster), `
	{
		"id":2,
		"name":"jack"
	}
	`); e != nil {
		t.Fatal(e)
	}

	if _, e := cli.Put(ctx, fmt.Sprintf("/v3/%s/%s/3", ModelType, Cluster), `
	{
		"id":3,
		"name":"jerry"
	}
	`); e != nil {
		t.Fatal(e)
	}
	if _, e := cli.Put(ctx, fmt.Sprintf("/v3/%s/%s/2", ModelType, Cluster), `
	{
		"id":2,
		"name":"harry"
	}
	`); e != nil {
		t.Fatal(e)
	}

	if _, e := cli.Delete(ctx, fmt.Sprintf("/v3/%s/%s/2", ModelType, Cluster)); e != nil {
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
