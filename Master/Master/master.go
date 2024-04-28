package Master

import (
	"container/list"
	"fmt"
	"go.etcd.io/etcd/client/v3"
	"log/slog"
	"strconv"
	"time"
)

const (
	etcdEndpoints = "http://localhost:2379"
	serverPrefix  = "/server/" // etcd key prefix for new region server
	regionPrefix  = "/region/" // etcd key prefix for new region
)

type Master struct {
	etcdClient *clientv3.Client
	regions    []*list.List        // regionID -> list of region servers, the first one is the primary server
	regionNum  int                 // number of regions
	servers    map[string]struct{} // all region servers set
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
	m.regions = make([]*list.List, 8)
	m.servers = make(map[string]struct{})

	// Watch for new region server
	go m.watchRegionServer()
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

	watchChan := watcher.Watch(m.etcdClient.Ctx(), serverPrefix, clientv3.WithPrefix(), clientv3.WithPrevKV())
	for {
		select {
		case resp := <-watchChan:
			for _, event := range resp.Events {
				switch event.Type {
				case clientv3.EventTypePut:
					ip := string(event.Kv.Key[len(serverPrefix):])
					regionID, err := strconv.Atoi(string(event.Kv.Value))
					if err != nil {
						slog.Error(fmt.Sprintf("Failed to convert region ID: %v", err))
						continue
					}

					// check if the server is already up
					if _, ok := m.servers[ip]; ok {
						continue
					}
					slog.Info(fmt.Sprintf("Region server up: %s", event.Kv.Key))
					m.servers[ip] = struct{}{}

					// link the region server to the region list
					if regionID >= len(m.regions) { // expand the regions list if necessary
						newRegions := make([]*list.List, max(regionID+1, 2*len(m.regions)))
						copy(newRegions, m.regions)
						m.regions = newRegions
					}
					isMaster := false
					if m.regions[regionID] == nil {
						m.regions[regionID] = list.New()
						isMaster = true
					}
					m.regions[regionID].PushBack(ip)
					m.regionNum = max(m.regionNum, regionID)

					// update the server info
					_, err = m.etcdClient.Put(m.etcdClient.Ctx(), regionPrefix+strconv.Itoa(regionID)+"/"+ip, strconv.FormatBool(isMaster))
					if err != nil {
						slog.Error(fmt.Sprintf("Failed to update region info: %v", err))
					}
				case clientv3.EventTypeDelete:
					ip := string(event.PrevKv.Key[len(serverPrefix):])
					regionID, err := strconv.Atoi(string(event.PrevKv.Value))
					if err != nil {
						slog.Error(fmt.Sprintf("Failed to convert region ID: %v", err))
						continue
					}

					// check if the server is already down
					if _, ok := m.servers[ip]; !ok {
						continue
					}
					slog.Info(fmt.Sprintf("Region server down: %s", event.Kv.Key))

					// TODO: 容错容灾 挂掉的设备应由多余设备进行补充

					if m.regions[regionID] != nil {
						for e := m.regions[regionID].Front(); e != nil; e = e.Next() {
							if e.Value.(string) == ip {
								if e == m.regions[regionID].Front() { // the master server is down
									if e.Next() != nil {
										// update the new master server
										slog.Info(fmt.Sprintf("New master server of region %d: %s", regionID, e.Next().Value.(string)))
										_, err = m.etcdClient.Put(m.etcdClient.Ctx(), regionPrefix+strconv.Itoa(regionID)+"/"+e.Next().Value.(string), "true")
										if err != nil {
											slog.Error(fmt.Sprintf("Failed to update region info: %v", err))
										}
									}
								}
								m.regions[regionID].Remove(e)
								break
							}
						}

						if m.regions[regionID].Len() == 0 {
							slog.Info(fmt.Sprintf("Region %d is removed", regionID))
							m.regions[regionID] = nil
						}
						// delete the server info
						_, err = m.etcdClient.Delete(m.etcdClient.Ctx(), regionPrefix+strconv.Itoa(regionID)+"/"+ip)
						if err != nil {
							slog.Error(fmt.Sprintf("Failed to delete region info: %v", err))
						}
					} else {
						slog.Error(fmt.Sprintf("Region %d is not found", regionID))
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
