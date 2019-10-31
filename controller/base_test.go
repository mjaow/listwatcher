package controller

import (
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/embed"
	"k8s.io/klog"
	"strings"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	StartEtcdServer()
	m.Run()
	defer StopEtcdServer()
}

var path = "/tmp/lw.etcd"

var LocalEtcdCfg = clientv3.Config{
	Endpoints:        []string{"127.0.0.1:2379"},
	AutoSyncInterval: time.Second * 3,
	DialTimeout:      time.Second * 3,
}

func NewOrDie(t *testing.T) *clientv3.Client {
	cli, err := clientv3.New(LocalEtcdCfg)
	if err != nil {
		t.Fatal(err)
	}
	return cli
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
