package server

import (
	"fmt"
	clientv3 "go.etcd.io/etcd/client/v3"
	"log/slog"
	"time"
)

type RegionServer struct {
	etcdClient *clientv3.Client // 通过 etcd 客户端操作 kv 键值对
	isMaster   bool             // 是否是当前 region 里的 master node

}

// ConnectToEtcd 连接到 etcd 集群
func (rs *RegionServer) ConnectToEtcd() {
	//连接到 etcd 集群
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		// handle error!
		slog.Error(fmt.Sprintf("Failed to connect to etcd: %v", err))
		return
	}
	fmt.Println("connect to etcd success")
	rs.etcdClient = cli

	// TODO：判断当前 server 在 region 内是 master 还是 slave

}

// ExitFromEtcd 退出 etcd 集群
func (rs *RegionServer) ExitFromEtcd() {

}
