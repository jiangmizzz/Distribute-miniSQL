package Master

import (
	"container/list"
	"encoding/json"
	"fmt"
	"go.etcd.io/etcd/client/v3"
	"log/slog"
	"time"
)

const (
	etcdEndpoints = "http://localhost:2379"
	regionsPrefix = "/regions/" // etcd key prefix for new region server
)

type Master struct {
	etcdClient *clientv3.Client
	regions    map[int]*list.List // 默认第一台设备为主设备
}

func (m *Master) Start() {
	slog.Info("Starting master...")
	// Connect to etcd
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{etcdEndpoints},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to connect to etcd: %v", err))
		return
	}
	m.etcdClient = cli

	// initialize member variables
	m.regions = make(map[int]*list.List)

	// Watch for new region server
	go m.watchRegionServer()
}

func decodeRegionServerInfo(data []byte) (map[string]interface{}, error) {
	var info map[string]interface{}
	err := json.Unmarshal(data, &info)
	if err != nil {
		return nil, err
	}
	return info, nil
}

func (m *Master) watchRegionServer() {
	slog.Info("Watching for new region server...")
	watcher := clientv3.NewWatcher(m.etcdClient)
	defer func(watcher clientv3.Watcher) {
		err := watcher.Close()
		if err != nil {
			slog.Error(fmt.Sprintf("Failed to close etcd watcher: %v", err))
			return
		}
	}(watcher)

	watchChan := watcher.Watch(m.etcdClient.Ctx(), regionsPrefix, clientv3.WithPrefix(), clientv3.WithPrevKV())
	for {
		select {
		case resp := <-watchChan:
			for _, event := range resp.Events {
				switch event.Type {
				case clientv3.EventTypePut:
					slog.Info(fmt.Sprintf("Region server up: %s", event.Kv.Key))
					data, err := decodeRegionServerInfo(event.Kv.Value)
					if err != nil {
						slog.Error(fmt.Sprintf("Failed to decode region server info: %v", err))
						continue
					}
					ip := string(event.Kv.Key[len(regionsPrefix):])
					regionID := int(data["region_id"].(float64)) // interface{} value type is float64

					// TODO: 为空闲server分配region

					if _, ok := m.regions[regionID]; !ok {
						m.regions[regionID] = list.New()
					}
					m.regions[regionID].PushBack(ip)
				case clientv3.EventTypeDelete:
					slog.Info(fmt.Sprintf("Region server down: %s", event.Kv.Key))
					data, err := decodeRegionServerInfo(event.PrevKv.Value)
					if err != nil {
						slog.Error(fmt.Sprintf("Failed to decode region server info: %v", err))
						continue
					}
					ip := string(event.PrevKv.Key[len(regionsPrefix):])
					regionID := int(data["region_id"].(float64))

					// TODO: 容错容灾 挂掉的设备应由多余设备进行补充

					if _, ok := m.regions[regionID]; ok {
						for e := m.regions[regionID].Front(); e != nil; e = e.Next() {
							if e.Value.(string) == ip {
								m.regions[regionID].Remove(e)
								break
							}
						}
						if m.regions[regionID].Len() == 0 {
							delete(m.regions, regionID)
						}
					} else {
						slog.Error(fmt.Sprintf("Region server not found: %d", regionID))
					}
				}
			}
		}
	}
}

func (m *Master) Stop() {
	slog.Info("Stopping master...")
	err := m.etcdClient.Close()
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to close etcd client: %v", err))
		return
	}
}
