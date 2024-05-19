package server

import (
	"Region/database"
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"time"

	"github.com/spf13/viper"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	etcdEndpoints  = "http://120.27.140.232:2379" // 现有集群中的节点地址
	discoverPrefix = "discovery"                  //服务发现前缀
	serverPrefix   = "server"                     // etcd key prefix for new region server
	regionPrefix   = "region"                     // etcd key prefix for new region
	tablePrefix    = "table"                      // etcd key prefix for new table
	visitPrefix    = "visit"                      // etcd key prefix for visit
)

var (
	currentAddr string            // 当前的 ip:port
	leaseTime   = 5 * time.Second // 租约有效时间
	Port        int               // 本server监听的端口号
	Rs          RegionServer      // rs实例
	configFile  string            //输入配置文件名
)

type RegionServer struct {
	etcdClient        *clientv3.Client                        // 通过 etcd 客户端操作 kv 键值对
	lease             *clientv3.LeaseGrantResponse            // 服务租约
	leaseAliveChannel <-chan *clientv3.LeaseKeepAliveResponse // 续约通道响应
	regionIdChannel   chan int                                // regionId 更新的通道
	stateWatcher      clientv3.Watcher
	IsMaster          bool             // 是否是当前 region 里的 master node
	RegionId          int              // 当前 server 所在 region 的id
	Visits            map[string]int   // 访问量统计 tableName-count
	ticker            <-chan time.Time //统计访问量的计时器
}

// ReadConfig 读取配置文件
func (rs *RegionServer) ReadConfig() {
	// 从命令行参数中指定配置文件名
	arg := flag.String("file", "config1", "specify config file name")
	flag.Parse()
	configFile = *arg
	viper.SetConfigName(configFile)
	viper.SetConfigType("yaml")
	viper.AddConfigPath("./server")
	err := viper.ReadInConfig()
	if err != nil {
		slog.Error("Fail to read server config file, %s", err)
	}
	//读取regionId, port, dbname
	rs.RegionId = viper.GetInt("server.region")
	Port = viper.GetInt("server.port")
	database.DBname = viper.GetString("server.dbname")
	viper.Reset()
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
		slog.Error(fmt.Sprintf("Failed to create lease: %v", err))
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
	// 注册服务
	rs.registerServer()
}

// 操作/server/discovery/ip - regionId 键值对，服务发现
// 注册 region server
func (rs *RegionServer) registerServer() {
	// 注册 server k-v (有租约）
	currentIp, _ := rs.getCurrentIp()
	if currentIp == "" {
		slog.Error("get current IP fail! Stop registering server. \n")
		return
	}
	currentAddr = currentIp + ":" + strconv.Itoa(Port) // 获取当前 ip:port
	fmt.Println(currentAddr)
	//服务发现
	serviceKey := fmt.Sprintf("/%s/%s", discoverPrefix, currentAddr)

	//写入 /server/find/ip:port - regionId 键值对
	_, err := rs.etcdClient.Put(rs.etcdClient.Ctx(), serviceKey, strconv.Itoa(rs.RegionId), clientv3.WithLease(rs.lease.ID)) //持有租约
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to register service: %v\n", err))
	}
	fmt.Println("Service registered successfully.")
	//开启通道，以在每次 regionId 更改后更新状态接收方法

	rs.regionIdChannel = make(chan int)
	go rs.watchState()
	rs.regionIdChannel <- rs.RegionId // regionId 写入通道

	// 监听 master 分配 regionId
	go rs.watchRegionId()
}

// 监听 /server/ip:port - regionId 中的 regionId 更改
func (rs *RegionServer) watchRegionId() {
	regionKey := fmt.Sprintf("/%s/%s", serverPrefix, currentAddr)
	regionChannel := rs.etcdClient.Watch(context.Background(), regionKey)
	fmt.Println("Start watching regionId...")
	for {
		select {
		case regionResp, ok := <-regionChannel:
			if !ok {
				return //通道关闭时退出
			}
			for _, ev := range regionResp.Events {
				regionId := string(ev.Kv.Value)
				newId, err := strconv.Atoi(regionId)
				if err != nil {
					slog.Error(fmt.Sprintf("Format of regionId is wrong! now: %s"))
				}
				if rs.RegionId != newId { //regionId 更新
					fmt.Printf("RegionId switched:  %d --> %d\n", rs.RegionId, newId)
					rs.RegionId = newId
					rs.regionIdChannel <- rs.RegionId //写入通道
				}
				// 将新 regionId 写入 config 文件
				viper.SetConfigName(configFile)
				viper.SetConfigType("yaml")
				viper.AddConfigPath("./server")
				viper.Set("server.region", rs.RegionId)
				viper.Set("server.port", Port)
				viper.Set("server.dbname", database.DBname)
				if err := viper.WriteConfig(); err != nil {
					panic(err)
				}
				// 更改regionId的情况不涉及表操作
				viper.Reset()
			}
		}
	}
}

// 监听 server 状态 (是 master 还是 slave)，
func (rs *RegionServer) watchState() {
	//等待通道中的 regionId 更新，关闭上一个state channel,重开一个监听
	for regionId := range rs.regionIdChannel {
		fmt.Printf("Receive new regionId: %d\n", regionId)
		//关闭上一个 watcher
		if rs.stateWatcher != nil {
			err := rs.stateWatcher.Close()
			if err != nil {
				slog.Error(fmt.Sprintf("Close watcher fail: %v", err))
				return
			}
			fmt.Println("Stop watching last region state.")
		}
		// 只有 regionId!=0 时才有必要监听
		if regionId == 0 {
			continue
		}
		stateKey := fmt.Sprintf("/%s/%d/%s", regionPrefix, regionId, currentAddr)
		value := rs.GetKey(stateKey)
		if value == "0" {
			rs.IsMaster = true
			fmt.Println("The server is a master server")
			go rs.initVisit()
		}
		rs.stateWatcher = clientv3.NewWatcher(rs.etcdClient) //建立监听
		stateChannel := rs.stateWatcher.Watch(context.Background(), stateKey)
		fmt.Printf("Start watching region state: regionId = %d\n", regionId)
		go func() {
			prevMaster := rs.IsMaster
			for stateResp := range stateChannel {
				for _, ev := range stateResp.Events {
					if string(ev.Kv.Value) == "0" {
						rs.IsMaster = true
						fmt.Println("The server is a master server")
						//slog.Info("The server is a master server")
					} else if _, err := strconv.Atoi(string(ev.Kv.Value)); err == nil {
						rs.IsMaster = false
					} else {
						slog.Error("value of server state is wrong!")
					}
				}
				if rs.IsMaster && !prevMaster { // 新转变为 master node，开始统计访问量
					go rs.initVisit()
				}
				prevMaster = rs.IsMaster
			}
		}()
	}
}

// 检查续约情况
func (rs *RegionServer) checkAlive() {
	fmt.Println("check server's alive state...")
	for {
		select {
		case resp := <-rs.leaseAliveChannel:
			if resp == nil { // 续约已关闭
				fmt.Println("KeepAlive channel closed")
				return
			} else {
				//fmt.Printf("Received keep alive response: TTL=%d\n", resp.TTL)
			}
		case <-time.After(leaseTime + leaseTime/2): //超时无响应
			fmt.Println("Keep alive timeout")
			return
		}
	}
}

// 访问量统计
func (rs *RegionServer) initVisit() {
	//初始化
	rs.Visits = make(map[string]int) //alloc
	fmt.Print("Start record visits: ")
	rs.getTables()
	//每隔 10s 上传一次访问量，然后刷新一遍 map
	if rs.ticker == nil {
		rs.ticker = time.Tick(10 * time.Second)
	}
	for range rs.ticker {
		if !rs.IsMaster {
			fmt.Print("Stop recording visits.\n")
			return //非 master 情况则退出监听
		}
		// 覆写前10s内的 /visit/tableName - count 统计量
		for k, v := range rs.Visits {
			visitKey := fmt.Sprintf("/%s/%s", visitPrefix, k)
			_, err := rs.etcdClient.Put(rs.etcdClient.Ctx(), visitKey, strconv.Itoa(v))
			if err != nil {
				slog.Error(fmt.Sprintf("Failed to update visit counting: %v\n", err))
			}
		}
		// 清空原 map 并更新
		rs.Visits = make(map[string]int)
		fmt.Print("Reset visit counts: ")
		rs.getTables()
	}
}

// 从 etcd 中获取该 region 现存的 tables
func (rs *RegionServer) getTables() {
	// 从 /table/tableName - regionId 中读取当前 server 中已经存在的表并置 visit 值为 0
	keyPrefix := "/" + tablePrefix + "/"
	resp, err := rs.etcdClient.Get(rs.etcdClient.Ctx(), keyPrefix, clientv3.WithPrefix())
	if err != nil {
		slog.Error(fmt.Sprintf("%s\n", err))
	} else {
		for _, kv := range resp.Kvs {
			if string(kv.Value) == strconv.Itoa(rs.RegionId) { //是当前region里的table
				tableName := string(kv.Key[len(keyPrefix):]) // 截取 tableName
				rs.Visits[tableName] = 0                     // reset
				fmt.Print(tableName + " ")
			}
		}
		fmt.Print("\n")
	}
}

// 获取当前 ip 地址
func (rs *RegionServer) getCurrentIp() (string, error) {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return "", err
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)
	return localAddr.IP.String(), nil
}

// ExitFromEtcd 退出 etcd 集群
func (rs *RegionServer) ExitFromEtcd() {
	slog.Info("Stop region server...")
	// close the etcd client
	if err := rs.etcdClient.Close(); err != nil {
		slog.Error(fmt.Sprintf("Failed to close etcd client: %s", err))
		return
	}
	return
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
	keyPrefix := "/region/" + strconv.Itoa(Rs.RegionId) + "/"
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
