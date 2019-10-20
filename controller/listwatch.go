package controller

import (
	"context"
	"github.com/coreos/etcd/clientv3"
)

type ListWatch interface {
	Watch(ctx context.Context, modVersion int64) clientv3.WatchChan

	List() (*clientv3.GetResponse, error)
}