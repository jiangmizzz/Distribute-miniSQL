package server

import (
	"context"
	"fmt"
	clientv3 "go.etcd.io/etcd/client/v3"
	"log"
	"log/slog"
	"net"
	"strconv"
	"time"
)

const (
	etcdEndpoints = "http://120.27.140.232:2379" // 现有集群中的节点地址
	serverPrefix  = "/server/"                   // etcd key prefix for new region server
	regionPrefix  = "/region/"                   // etcd key prefix for new region
)

var (
	regionId  int               // 当前 server 所在 region 的id
	currentIp string            // 当前的 ip 地址
	leaseTime = 5 * time.Second // 租约有效时间
	Rs        RegionServer
)

type RegionServer struct {
	etcdClient        *clientv3.Client                        // 通过 etcd 客户端操作 kv 键值对
	lease             *clientv3.LeaseGrantResponse            // 服务租约
	leaseAliveChannel <-chan *clientv3.LeaseKeepAliveResponse // 续约通道响应
	isMaster          bool                                    // 是否是当前 region 里的 master node
}

// ConnectToEtcd 连接到 etcd 集群
func (rs *RegionServer) ConnectToEtcd() {
	//连接到 etcd 集群
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{etcdEndpoints},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		// handle error!
		slog.Error(fmt.Sprintf("Failed to connect to etcd: %v", err))
		return
	}
	fmt.Println("connect to etcd success")
	rs.etcdClient = cli

	// 创建租约
	leaseResp, err := cli.Grant(context.Background(), int64(leaseTime.Seconds()))
	if err != nil {
		slog.Error("Failed to create lease:", err)
		return
	}
	fmt.Println("create lease success")
	rs.lease = leaseResp

	// 持续续约
	aliveChannel, err := rs.etcdClient.KeepAlive(context.Background(), rs.lease.ID)
	if err != nil {
		fmt.Println("Failed to renew lease:", err)
	}
	rs.leaseAliveChannel = aliveChannel

	// 在新 goroutine 中检查续约情况
	go rs.checkAlive()

	rs.registerServer()
}

// 注册 region server
func (rs *RegionServer) registerServer() {
	// 注册 server k-v (有租约）
	currentIp = rs.getCurrentIp() // 获取当前 ip
	serviceKey := fmt.Sprintf("%s%s", serverPrefix, currentIp)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	// TODO: 获取 region id, 暂时 mock 一下
	regionId = 1
	_, err := rs.etcdClient.Put(ctx, serviceKey, strconv.Itoa(regionId), clientv3.WithLease(rs.lease.ID))
	cancel()
	if err != nil {
		log.Fatal("Failed to register service:", err)
	}
	fmt.Println("Service registered successfully.")

	// TODO：判断当前 server 在 region 内是 master 还是 slave

}

// 检查续约情况
func (rs *RegionServer) checkAlive() {
	for {
		select {
		case resp := <-rs.leaseAliveChannel:
			if resp == nil { // 续约已关闭
				fmt.Println("KeepAlive channel closed")
				return
			} else {
				fmt.Printf("Received keep alive response: TTL=%d\n", resp.TTL)
			}
		case <-time.After(leaseTime + leaseTime/2): //超时无响应
			fmt.Println("Keep alive timeout")
			return
		}
	}
}

// 获取当前 ip 地址
func (rs *RegionServer) getCurrentIp() string {
	// 获取所有网络接口信息
	interfaces, err := net.Interfaces()
	if err != nil {
		fmt.Println("Failed to get network interfaces:", err)
		return ""
	}

	// 遍历所有网络接口
	for _, iface := range interfaces {
		// 排除回环接口和虚拟接口
		if iface.Flags&net.FlagLoopback == 0 && iface.Flags&net.FlagUp != 0 {
			addrs, err := iface.Addrs()
			if err != nil {
				fmt.Println("Failed to get IP addresses:", err)
				continue
			}
			// 遍历接口的所有 IP 地址
			for _, addr := range addrs {
				// 检查地址类型是否是 IP 地址 (这里只找 ipv4 地址)
				if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() && ipnet.IP.To4() != nil {
					fmt.Println("Current IP address:", ipnet.IP.String())
					return ipnet.IP.String() // 获取到一个地址后立即返回
				}
			}
		}
	}
	return ""
}

// ExitFromEtcd 退出 etcd 集群
func (rs *RegionServer) ExitFromEtcd() {

}

func (rs *RegionServer) PutKey(key string, value string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, err := rs.etcdClient.Put(ctx, key, value)
	cancel()
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to put key %s with value %s: %v", key, value, err))
	}
}

func (rs *RegionServer) GetKey(key string) string {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	resp, err := rs.etcdClient.Get(ctx, key)
	cancel()
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to get key %s: %v", key, err))
		return ""
	}
	if len(resp.Kvs) == 0 {
		return ""
	}
	return string(resp.Kvs[0].Value)
}

func (rs *RegionServer) DeleteKey(key string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, err := rs.etcdClient.Delete(ctx, key)
	cancel()
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to delete key %s: %v", key, err))
	}
}

func (rs *RegionServer) GetSlaves() []string {
	keyPrefix := "/region/" + strconv.Itoa(regionId) + "/"
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	resp, err := rs.etcdClient.Get(ctx, keyPrefix, clientv3.WithPrefix())
	cancel()
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to get key %s: %v", keyPrefix, err))
		return nil
	}
	slaves := make([]string, 0)
	for _, kv := range resp.Kvs {
		if string(kv.Value) != "0" {
			slaves = append(slaves, string(kv.Key[len(keyPrefix):]))
		}
	}
	return slaves
}
