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
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	etcdEndpoints = "http://localhost:2379"
	serverPrefix  = "/server/" // etcd key prefix for new region server
	regionPrefix  = "/region/" // etcd key prefix for new region
	tablePrefix   = "/table/"

	regionServerNum    = 3 // the number of region servers in a region
	backupServerNum    = 2 // the number of idle servers for backup
	regionMinServerNum = 2 // the minimum number of region servers in a region (first region)
)

type Master struct {
	controller Controller.Controller
	server     *http.Server
	etcdClient *clientv3.Client
	regions    map[int]*list.List  // regionID -> list of region servers, the first one is the primary server
	servers    map[string]struct{} // all region servers set
	tables     map[string]int      // table name -> region ID
	tableNum   map[int]int         // region ID -> table number
}

func (m *Master) Start() {
	slog.Info("Starting master...")
	// connect to etcd
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
	m.regions = make(map[int]*list.List)
	m.servers = make(map[string]struct{})
	m.tables = make(map[string]int)
	m.tableNum = make(map[int]int)

	// watch for new region server
	go m.watchRegionServer()
	// watch for new table
	go m.watchTable()

	// start the backend server
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

	// get the server already in etcd
	m.getInitInfoFromEtcd()
}

func (m *Master) setupRouter() *gin.Engine {
	gin.SetMode(gin.ReleaseMode)

	r := gin.Default()

	r.Use(Route.CorsMiddleware())

	apiRoutes := r.Group("/api")
	apiRoutes.POST("/table/query", m.controller.QueryTable)
	apiRoutes.GET("/table/new", m.controller.NewTable)
	apiRoutes.GET("/table/delete", m.controller.DeleteTable)
	return r
}

func (m *Master) getInitInfoFromEtcd() {
	// get the server already in etcd
	resp, err := m.etcdClient.Get(m.etcdClient.Ctx(), serverPrefix, clientv3.WithPrefix())
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to get server info: %v", err))
		return
	}
	for _, kv := range resp.Kvs {
		ip := string(kv.Key[len(serverPrefix):])
		regionID, err := strconv.Atoi(string(kv.Value))
		if err != nil {
			slog.Error(fmt.Sprintf("Failed to convert region ID: %v", err))
			continue
		}
		m.serverUp(ip, regionID, false)
	}

	// correct the region info(master/slave)
	resp, err = m.etcdClient.Get(m.etcdClient.Ctx(), regionPrefix, clientv3.WithPrefix())
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to get region info: %v", err))
		return
	}
	for _, kv := range resp.Kvs {
		parts := strings.Split(string(kv.Key[len(regionPrefix):]), "/")
		if len(parts) != 2 {
			slog.Error(fmt.Sprintf("Invalid region info: %s", string(kv.Key)))
			continue
		}
		regionID, err := strconv.Atoi(parts[0])
		if err != nil {
			slog.Error(fmt.Sprintf("Failed to convert region ID: %v", err))
			continue
		}
		ip := parts[1]

		isMaster, err := strconv.ParseBool(string(kv.Value))
		if err != nil {
			slog.Error(fmt.Sprintf("Failed to convert master info: %v", err))
			continue
		}
		if isMaster {
			for e := m.regions[regionID].Front(); e != nil; e = e.Next() {
				if e.Value.(string) == ip {
					m.regions[regionID].MoveToFront(e)
					break
				}
			}
		}
	}

	// get the table already in etcd
	resp, err = m.etcdClient.Get(m.etcdClient.Ctx(), tablePrefix, clientv3.WithPrefix())
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to get table info: %v", err))
		return
	}
	for _, kv := range resp.Kvs {
		tableName := string(kv.Key[len(tablePrefix):])
		regionID, err := strconv.Atoi(string(kv.Value))
		if err != nil {
			slog.Error(fmt.Sprintf("Failed to convert region ID: %v", err))
			continue
		}
		m.tableCreate(tableName, regionID)
	}
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
					m.serverUp(ip, regionID, true)
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

// isNew is false if the server is already up when the master starts
func (m *Master) serverUp(ip string, regionID int, isNew bool) {
	// check if the server is already up
	if _, ok := m.servers[ip]; ok {
		return
	}
	slog.Info(fmt.Sprintf("Region server up: %s", ip))
	m.servers[ip] = struct{}{}

	// link the region server to the region list
	isMaster := false
	if m.regions[regionID] == nil {
		m.regions[regionID] = list.New()
		isMaster = true
	}
	m.regions[regionID].PushBack(ip)
	if _, ok := m.tableNum[regionID]; !ok && regionID != 0 {
		m.tableNum[regionID] = 0
	}

	if isNew && regionID != 0 {
		// update the server info if the server in a region
		_, err := m.etcdClient.Put(m.etcdClient.Ctx(), regionPrefix+strconv.Itoa(regionID)+"/"+ip, strconv.FormatBool(isMaster))
		if err != nil {
			slog.Error(fmt.Sprintf("Failed to update region info: %v", err))
		}
	}
}

func (m *Master) serverDown(ip string, regionID int) {
	// check if the server is already down
	if _, ok := m.servers[ip]; !ok {
		return
	}
	slog.Info(fmt.Sprintf("Region server down: %s", ip))

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
				// remove the server from the metadata
				m.regions[regionID].Remove(e)
				delete(m.servers, ip)
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

		// try to get an idle server to replace the down server if necessary
		if regionID != 0 && m.regions[regionID] != nil && m.regions[regionID].Len() < regionServerNum && m.regions[0] != nil && m.regions[0].Len() > 0 {
			newServer := m.regions[0].Front().Value.(string)
			m.regions[0].Remove(m.regions[0].Front())
			// update the server info
			if _, err := m.etcdClient.Put(m.etcdClient.Ctx(), serverPrefix+newServer, strconv.Itoa(regionID)); err != nil {
				slog.Error(fmt.Sprintf("Failed to update server info: %v", err))
			}
			// update the region info
			if _, err := m.etcdClient.Put(m.etcdClient.Ctx(), regionPrefix+strconv.Itoa(regionID)+"/"+newServer, "false"); err != nil {
				slog.Error(fmt.Sprintf("Failed to update region info: %v", err))
			}
			m.regions[regionID].PushBack(newServer)
			slog.Info(fmt.Sprintf("Region server %s is assigned to region %d", newServer, regionID))
		}
	} else {
		slog.Error(fmt.Sprintf("Region %d is not found", regionID))
	}
}

func (m *Master) watchTable() {
	slog.Info("Watching for new table...")
	watcher := clientv3.NewWatcher(m.etcdClient)
	defer func(watcher clientv3.Watcher) {
		err := watcher.Close()
		if err != nil {
			slog.Error(fmt.Sprintf("Failed to close etcd watcher: %v", err))
			return
		}
	}(watcher)

	watchChan := watcher.Watch(m.etcdClient.Ctx(), tablePrefix, clientv3.WithPrefix(), clientv3.WithPrevKV())
	for {
		select {
		case resp := <-watchChan:
			for _, event := range resp.Events {
				switch event.Type {
				case clientv3.EventTypePut:
					tableName := string(event.Kv.Key[len(tablePrefix):])
					regionID, err := strconv.Atoi(string(event.Kv.Value))
					if err != nil {
						slog.Error(fmt.Sprintf("Failed to convert region ID: %v", err))
						continue
					}
					m.tableCreate(tableName, regionID)
				case clientv3.EventTypeDelete:
					tableName := string(event.PrevKv.Key[len(tablePrefix):])
					m.tableDelete(tableName)
				}
			}
		}
	}
}

func (m *Master) tableCreate(tableName string, regionID int) {
	if _, ok := m.tables[tableName]; !ok {
		slog.Info(fmt.Sprintf("Table %s is created in region %d", tableName, regionID))
		m.tableNum[regionID]++
	} else {
		slog.Info(fmt.Sprintf("Table %s is moved to region %d", tableName, regionID))
	}
	m.tables[tableName] = regionID
}

func (m *Master) tableDelete(tableName string) {
	if regionID, ok := m.tables[tableName]; ok {
		slog.Info(fmt.Sprintf("Table %s is removed from region %d", tableName, regionID))
		m.tableNum[regionID]--
		delete(m.tables, tableName)
		// if the region is empty, remove it
		if m.tableNum[regionID] == 0 {
			// delete the region info
			if _, err := m.etcdClient.Delete(m.etcdClient.Ctx(), regionPrefix+strconv.Itoa(regionID), clientv3.WithPrefix()); err != nil {
				slog.Error(fmt.Sprintf("Failed to delete region info: %v", err))
			}

			// check if the region is existed
			if regionID == 0 {
				slog.Warn("Region 0 is not valid")
				return
			}
			if _, ok := m.regions[regionID]; !ok {
				slog.Warn(fmt.Sprintf("Region %d is not found", regionID))
				return
			}
			// update the server info
			for e := m.regions[regionID].Front(); e != nil; e = e.Next() {
				if _, err := m.etcdClient.Put(m.etcdClient.Ctx(), serverPrefix+e.Value.(string), "0"); err != nil {
					slog.Error(fmt.Sprintf("Failed to update server info: %v", err))
				}
				m.regions[0].PushBack(e.Value)
			}
			delete(m.regions, regionID)
			delete(m.tableNum, regionID)
		}
	} else {
		slog.Warn(fmt.Sprintf("Table %s is not found", tableName))
	}
}

func (m *Master) QueryTable(tableNames []string) ([]dto.QueryTableResponse, error) {
	var tables []dto.QueryTableResponse
	for _, tableName := range tableNames {
		// get the region stored the table
		resp, err := m.etcdClient.Get(m.etcdClient.Ctx(), tablePrefix+tableName)
		if err != nil {
			slog.Error(fmt.Sprintf("Failed to get table info: %v", err))
			return nil, err
		}
		tables = append(tables, dto.QueryTableResponse{
			Name: tableName,
			IP:   "",
		})

		// get the master server of the region if exist
		if resp.Count == 0 {
			slog.Warn(fmt.Sprintf("Table %s is not found.", tableName))
			continue
		}
		regionID := int(resp.Kvs[0].Value[0])
		if !m.checkRegionSafety(regionID) {
			continue
		}
		tables[len(tables)-1].IP = m.regions[regionID].Front().Value.(string)
	}
	return tables, nil
}

func (m *Master) NewTable(tableName string) (string, error) {
	// check if the table is already exist
	if _, ok := m.tables[tableName]; ok {
		slog.Warn(fmt.Sprintf("Table %s is already exist.", tableName))
		return "", errors.New("table already exist")
	}

	// find the region to store the table
	regionID := 0
	// find if there is a region with no table
	for id, num := range m.tableNum {
		if num == 0 {
			regionID = id
			break
		}
	}
	// if there is no region with no table
	if regionID == 0 {
		// if there are enough idle region servers
		if m.regions[0] != nil && m.regions[0].Len() >= backupServerNum+regionServerNum {
			regionID = m.createNewRegion()
		} else {
			// find the region with the least tables
			minTableNum := math.MaxInt32
			for id, num := range m.tableNum {
				if num < minTableNum {
					minTableNum = num
					regionID = id
				}
			}
		}
		// if there is still no region
		if regionID == 0 {
			if m.regions[0] != nil && m.regions[0].Len() >= regionMinServerNum {
				regionID = m.createNewRegion()
			} else {
				return "", errors.New("no enough region servers")
			}
		}
	}
	if !m.checkRegionSafety(regionID) {
		return "", errors.New("no enough region servers")
	}
	return m.regions[regionID].Front().Value.(string), nil
}

func (m *Master) createNewRegion() int {
	// Get the new region servers
	var newRegionServers []string
	for i := 0; i < min(regionServerNum, len(m.servers)); i++ {
		newRegionServers = append(newRegionServers, m.regions[0].Front().Value.(string))
		m.regions[0].Remove(m.regions[0].Front())
	}

	// Create a new region
	// Find the first empty region
	regionID := 1
	for ; regionID <= len(m.regions); regionID++ {
		if m.regions[regionID] == nil {
			break
		}
	}
	// Link the new region servers to the new region
	m.regions[regionID] = list.New()
	for i, ip := range newRegionServers {
		m.regions[regionID].PushBack(ip)
		if _, err := m.etcdClient.Put(m.etcdClient.Ctx(), regionPrefix+strconv.Itoa(regionID)+"/"+ip, strconv.FormatBool(i == 0)); err != nil {
			slog.Error(fmt.Sprintf("Failed to update region info: %v", err))
		}
	}
	for _, ip := range newRegionServers {
		if _, err := m.etcdClient.Put(m.etcdClient.Ctx(), serverPrefix+ip, strconv.Itoa(regionID)); err != nil {
			slog.Error(fmt.Sprintf("Failed to update server info: %v", err))
		}
	}
	m.tableNum[regionID] = 0
	slog.Info(fmt.Sprintf("New region %d is created: %v", regionID, newRegionServers))
	return regionID
}

func (m *Master) DeleteTable(tableName string) (string, error) {
	// check if the table exists
	if _, ok := m.tables[tableName]; !ok {
		slog.Warn(fmt.Sprintf("Table %s is not exist.", tableName))
		return "", errors.New("table not exist")
	}

	// get the region stored the table
	regionID := m.tables[tableName]
	if !m.checkRegionSafety(regionID) {
		return "", errors.New("no enough region servers")
	}
	return m.regions[regionID].Front().Value.(string), nil
}

// ensure the region has at least `regionMinServerNum` servers
func (m *Master) checkRegionSafety(regionID int) bool {
	if m.regions[regionID] == nil || m.regions[regionID].Len() == 0 {
		slog.Error(fmt.Sprintf("Region %d is not found", regionID))
		return false
	}

	for m.regions[regionID].Len() < regionMinServerNum {
		// try to assign idle servers to the region
		if m.regions[0] != nil && m.regions[0].Len() > 0 {
			newServer := m.regions[0].Front().Value.(string)
			m.regions[0].Remove(m.regions[0].Front())
			// update the server info
			if _, err := m.etcdClient.Put(m.etcdClient.Ctx(), serverPrefix+newServer, strconv.Itoa(regionID)); err != nil {
				slog.Error(fmt.Sprintf("Failed to update server info: %v", err))
				return false
			}
			// update the region info
			if _, err := m.etcdClient.Put(m.etcdClient.Ctx(), regionPrefix+strconv.Itoa(regionID)+"/"+newServer, "false"); err != nil {
				slog.Error(fmt.Sprintf("Failed to update region info: %v", err))
				return false
			}
			m.regions[regionID].PushBack(newServer)
			slog.Info(fmt.Sprintf("Region server %s is assigned to region %d", newServer, regionID))
		} else {
			slog.Warn(fmt.Sprintf("Region %d has no enough servers", regionID))
			return false
		}
	}
	return true
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