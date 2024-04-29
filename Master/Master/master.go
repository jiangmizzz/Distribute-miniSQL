package Master

import (
	"Master/Controller"
	"Master/api/dto"
	"Master/route"
	"container/list"
	"context"
	"errors"
	"fmt"
	"github.com/gin-gonic/gin"
	"go.etcd.io/etcd/client/v3"
	"log/slog"
	"net/http"
	"strconv"
	"time"
)

const (
	etcdEndpoints = "http://localhost:2379"
	serverPrefix  = "/server/" // etcd key prefix for new region server
	regionPrefix  = "/region/" // etcd key prefix for new region
	tablePrefix   = "/table/"
)

type Master struct {
	controller Controller.Controller
	server     *http.Server
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
	m.controller = Controller.Controller{Service: m}
	m.regions = make([]*list.List, 8)
	m.servers = make(map[string]struct{})

	// Watch for new region server
	go m.watchRegionServer()

	// Start the server
	router := m.setupRouter()
	m.server = &http.Server{
		Addr:    ":8080",
		Handler: router,
	}
	go func() {
		if err = m.server.ListenAndServe(); err != nil && !errors.Is(http.ErrServerClosed, err) {
			slog.Error(fmt.Sprintf("Failed to run server: %v", err))
		}
	}()

	// Get the region already in etcd
	resp, err := m.etcdClient.Get(m.etcdClient.Ctx(), regionPrefix, clientv3.WithPrefix())
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to get region info: %v", err))
		return
	}
	for _, kv := range resp.Kvs {
		key := string(kv.Key)
		value := string(kv.Value)
		regionID, err := strconv.Atoi(key[len(regionPrefix) : len(regionPrefix)+1])
		if err != nil {
			slog.Error(fmt.Sprintf("Failed to convert region ID: %v", err))
			continue
		}
		ip := key[len(regionPrefix)+2:]
		isMaster, err := strconv.ParseBool(value)
		if err != nil {
			slog.Error(fmt.Sprintf("Failed to convert isMaster: %v", err))
			continue
		}
		m.serverUp(ip, regionID)
		if isMaster {
			slog.Info(fmt.Sprintf("Master server of region %d: %s", regionID, ip))
			// move the master server to the front
			for e := m.regions[regionID].Front(); e != nil; e = e.Next() {
				if e.Value.(string) == ip {
					m.regions[regionID].MoveToFront(e)
					break
				}
			}
		}
	}
}

func (m *Master) setupRouter() *gin.Engine {
	//gin.SetMode(gin.ReleaseMode)

	r := gin.Default()

	r.Use(Route.CorsMiddleware())

	apiRoutes := r.Group("/api")
	apiRoutes.POST("/table/query", m.controller.QueryTable)
	return r
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
					m.serverUp(ip, regionID)
				case clientv3.EventTypeDelete:
					ip := string(event.PrevKv.Key[len(serverPrefix):])
					regionID, err := strconv.Atoi(string(event.PrevKv.Value))
					if err != nil {
						slog.Error(fmt.Sprintf("Failed to convert region ID: %v", err))
						continue
					}
					m.serverDown(ip, regionID)
				}
			}
		}
	}
}

func (m *Master) serverUp(ip string, regionID int) {
	// check if the server is already up
	if _, ok := m.servers[ip]; ok {
		return
	}
	slog.Info(fmt.Sprintf("Region server up: %s", ip))
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
	_, err := m.etcdClient.Put(m.etcdClient.Ctx(), regionPrefix+strconv.Itoa(regionID)+"/"+ip, strconv.FormatBool(isMaster))
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to update region info: %v", err))
	}
}

func (m *Master) serverDown(ip string, regionID int) {
	// check if the server is already down
	if _, ok := m.servers[ip]; !ok {
		return
	}
	slog.Info(fmt.Sprintf("Region server down: %s", ip))

	// TODO: 容错容灾 挂掉的设备应由多余设备进行补充

	if m.regions[regionID] != nil {
		for e := m.regions[regionID].Front(); e != nil; e = e.Next() {
			if e.Value.(string) == ip {
				if e == m.regions[regionID].Front() { // the master server is down
					if e.Next() != nil {
						// update the new master server
						slog.Info(fmt.Sprintf("New master server of region %d: %s", regionID, e.Next().Value.(string)))
						if _, err := m.etcdClient.Put(m.etcdClient.Ctx(), regionPrefix+strconv.Itoa(regionID)+"/"+e.Next().Value.(string), "true"); err != nil {
							slog.Error(fmt.Sprintf("Failed to update region info: %v", err))
						}
					}
				}
				m.regions[regionID].Remove(e)
				break
			}
		}
		// delete the region list if it is empty
		if m.regions[regionID].Len() == 0 {
			slog.Info(fmt.Sprintf("Region %d is removed", regionID))
			m.regions[regionID] = nil
		}
		// delete the server info
		if _, err := m.etcdClient.Delete(m.etcdClient.Ctx(), regionPrefix+strconv.Itoa(regionID)+"/"+ip); err != nil {
			slog.Error(fmt.Sprintf("Failed to delete region info: %v", err))
		}
	} else {
		slog.Error(fmt.Sprintf("Region %d is not found", regionID))
	}
}

func (m *Master) QueryTable(tableNames []string) ([]dto.Tables, error) {
	var tables []dto.Tables
	for _, tableName := range tableNames {
		// Get the region stored the table
		resp, err := m.etcdClient.Get(m.etcdClient.Ctx(), tablePrefix+tableName)
		if err != nil {
			slog.Error(fmt.Sprintf("Failed to get table info: %v", err))
			return nil, err
		}
		if resp.Count == 0 {
			slog.Warn(fmt.Sprintf("Table %s is not found.", tableName))
			tables = append(tables, dto.Tables{
				Name: tableName,
				IP:   "",
			})
		} else {
			tables = append(tables, dto.Tables{
				Name: tableName,
				IP:   string(resp.Kvs[0].Value),
			})
		}
	}
	return tables, nil
}

func (m *Master) Stop() {
	slog.Info("Stopping master...")
	// Close the etcd client
	if err := m.etcdClient.Close(); err != nil {
		slog.Error(fmt.Sprintf("Failed to close etcd client: %v", err))
		return
	}

	// Shutdown the server
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := m.server.Shutdown(ctx); err != nil {
		slog.Error(fmt.Sprintf("Failed to stop server: %v", err))
	}
}
