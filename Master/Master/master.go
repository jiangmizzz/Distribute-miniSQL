package Master

import (
	"Master/Controller"
	"Master/api/dto"
	"Master/route"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gin-gonic/gin"
	"go.etcd.io/etcd/client/v3"
	"io"
	"log/slog"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	etcdEndpoints  = "http://localhost:2379"
	discoverPrefix = "/server/discovery/"
	serverPrefix   = "/server/" // etcd key prefix for new region server
	regionPrefix   = "/region/" // etcd key prefix for new region
	tablePrefix    = "/table/"
	visitPrefix    = "/visit/"

	movePath = "/table/move"

	regionServerNum      = 3   // the number of region servers in a region
	backupServerNum      = 2   // the number of idle servers for backup
	regionMinServerNum   = 2   // the minimum number of region servers in a region (first region)
	loadBalanceCycle     = 5   // the cycle of load balance (minutes)
	migrateThresholdHigh = 1.2 // if a region has more than 20% of the average visit times, it is considered as a hot region
	migrateThresholdLow  = 0.8 // if a region has less than 20% of the average visit times, it is considered as a cold region
)

type Master struct {
	controller     Controller.Controller
	server         *http.Server
	etcdClient     *clientv3.Client
	ticker         *time.Ticker
	regions        map[int][]string       // regionID -> array of region servers, the first one is the primary server
	servers        map[string]struct{}    // all region servers set
	tables         map[string]int         // table name -> region ID
	tableNum       map[int]int            // region ID -> table number
	visitNum       map[int]map[string]int // region ID -> table name -> visit times
	regionVisitNum map[int]int            // region ID -> total visit times
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
	m.regions = make(map[int][]string)
	m.servers = make(map[string]struct{})
	m.tables = make(map[string]int)
	m.tableNum = make(map[int]int)
	m.visitNum = make(map[int]map[string]int)
	m.regionVisitNum = make(map[int]int)

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

	// set ticker to get the visit times of each table
	m.ticker = time.NewTicker(loadBalanceCycle * time.Minute)
	go func() {
		for range m.ticker.C {
			m.getVisitTimes()
		}
	}()
}

func (m *Master) setupRouter() *gin.Engine {
	gin.SetMode(gin.ReleaseMode)

	r := gin.Default()

	r.Use(Route.CorsMiddleware())

	apiRoutes := r.Group("/api")
	apiRoutes.POST("/table/query", m.controller.QueryTable)
	apiRoutes.GET("/table/new", m.controller.NewTable)
	apiRoutes.GET("/table/delete", m.controller.DeleteTable)
	apiRoutes.GET("/table/show", m.controller.ShowTable)
	return r
}

func (m *Master) getInitInfoFromEtcd() {
	serverIndex := make(map[int]map[string]int)
	// get the region info (index in the region)
	resp, err := m.etcdClient.Get(m.etcdClient.Ctx(), regionPrefix, clientv3.WithPrefix())
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

		index, err := strconv.Atoi(string(kv.Value))
		if err != nil {
			slog.Error(fmt.Sprintf("Failed to convert master info: %v", err))
			continue
		}
		if serverIndex[regionID] == nil {
			serverIndex[regionID] = make(map[string]int)
		}
		serverIndex[regionID][ip] = index
	}

	// get the server already in etcd
	// get modified server's regionID first
	serverRegion := make(map[string]int)
	resp, err = m.etcdClient.Get(m.etcdClient.Ctx(), serverPrefix, clientv3.WithPrefix())
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
		serverRegion[ip] = regionID
	}
	// then get all discovered server info
	resp, err = m.etcdClient.Get(m.etcdClient.Ctx(), discoverPrefix, clientv3.WithPrefix())
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to get discovered server info: %v", err))
		return
	}
	for _, kv := range resp.Kvs {
		ip := string(kv.Key[len(discoverPrefix):])
		regionID, err := strconv.Atoi(string(kv.Value))
		if err != nil {
			slog.Error(fmt.Sprintf("Failed to convert region ID: %v", err))
			continue
		}
		if id, ok := serverRegion[ip]; ok {
			regionID = id
		}
		index := -1
		if i, ok := serverIndex[regionID][ip]; ok {
			index = i
		}
		m.serverUpWhenInit(ip, regionID, index)
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

	watchChan := watcher.Watch(m.etcdClient.Ctx(), discoverPrefix, clientv3.WithPrefix(), clientv3.WithPrevKV())
	for {
		select {
		case resp := <-watchChan:
			for _, event := range resp.Events {
				switch event.Type {
				case clientv3.EventTypePut:
					ip := string(event.Kv.Key[len(discoverPrefix):])
					regionID, err := strconv.Atoi(string(event.Kv.Value))
					if err != nil {
						slog.Error(fmt.Sprintf("Failed to convert region ID: %v", err))
						continue
					}
					m.serverUp(ip, regionID)
				case clientv3.EventTypeDelete:
					ip := string(event.PrevKv.Key[len(discoverPrefix):])
					resp, err := m.etcdClient.Get(m.etcdClient.Ctx(), serverPrefix+ip)
					if err != nil {
						slog.Error(fmt.Sprintf("Failed to get server info: %v", err))
						continue
					}
					regionID, err := strconv.Atoi(string(resp.Kvs[0].Value))
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

// server up before master up
func (m *Master) serverUpWhenInit(ip string, regionID int, index int) {
	// check if the server is already up
	if _, ok := m.servers[ip]; ok {
		return
	}
	slog.Info(fmt.Sprintf("Region server up: %s", ip))
	m.servers[ip] = struct{}{}

	// add the region server to the region array
	if m.regions[regionID] == nil { // pre-allocate the region array to speed up
		m.regions[regionID] = make([]string, index+1, regionServerNum)
	}
	if index >= len(m.regions[regionID]) {
		for i := len(m.regions[regionID]); i <= index; i++ {
			m.regions[regionID] = append(m.regions[regionID], "")
		}
	}
	if index == -1 {
		m.regions[regionID] = append(m.regions[regionID], ip)
	} else {
		m.regions[regionID][index] = ip
	}
	if _, ok := m.tableNum[regionID]; !ok && regionID != 0 {
		m.tableNum[regionID] = 0
	}
}

func (m *Master) serverUp(ip string, regionID int) {
	// check if the server is already up
	if _, ok := m.servers[ip]; ok {
		return
	}
	slog.Info(fmt.Sprintf("Region server up: %s", ip))
	m.servers[ip] = struct{}{}

	// if the region is full, assign the server to the idle list
	if regionID != 0 && len(m.regions[regionID]) >= regionServerNum {
		if _, err := m.etcdClient.Put(m.etcdClient.Ctx(), serverPrefix+ip, "0"); err != nil {
			slog.Error(fmt.Sprintf("Failed to update server info: %v", err))
		}
		m.regions[0] = append(m.regions[0], ip)
		slog.Info(fmt.Sprintf("Region %d is full, server %s is assigned to idle list", regionID, ip))
		return
	} else {
		if _, err := m.etcdClient.Put(m.etcdClient.Ctx(), serverPrefix+ip, strconv.Itoa(regionID)); err != nil {
			slog.Error(fmt.Sprintf("Failed to update server info: %v", err))
		}
	}

	// add the region server to the region array
	if m.regions[regionID] == nil { // pre-allocate the region array to speed up
		m.regions[regionID] = make([]string, 0, regionServerNum)
	}
	m.regions[regionID] = append(m.regions[regionID], ip)
	if _, ok := m.tableNum[regionID]; !ok && regionID != 0 {
		m.tableNum[regionID] = 0
	}

	if regionID != 0 {
		// update the server info if the server in a region
		_, err := m.etcdClient.Put(m.etcdClient.Ctx(), regionPrefix+strconv.Itoa(regionID)+"/"+ip, strconv.Itoa(len(m.regions[regionID])-1))
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
	// delete server info
	if _, err := m.etcdClient.Delete(m.etcdClient.Ctx(), serverPrefix+ip); err != nil {
		slog.Error(fmt.Sprintf("Failed to delete server info: %v", err))
	}

	if m.regions[regionID] != nil {
		for i, s := range m.regions[regionID] {
			if s == ip {
				// update successor server's index
				if regionID != 0 {
					for j := i + 1; j < len(m.regions[regionID]); j++ {
						if _, err := m.etcdClient.Put(m.etcdClient.Ctx(), regionPrefix+strconv.Itoa(regionID)+"/"+m.regions[regionID][j], strconv.Itoa(j-1)); err != nil {
							slog.Error(fmt.Sprintf("Failed to update region info: %v", err))
						}
					}
				}
				// remove the server from the metadata
				m.regions[regionID] = append(m.regions[regionID][:i], m.regions[regionID][i+1:]...)
				delete(m.servers, ip)
				break
			}
		}
		// delete the region list if it is empty
		if len(m.regions[regionID]) == 0 {
			slog.Info(fmt.Sprintf("Region %d is removed", regionID))
			m.regions[regionID] = nil
		}
		// delete the server info
		if _, err := m.etcdClient.Delete(m.etcdClient.Ctx(), regionPrefix+strconv.Itoa(regionID)+"/"+ip); err != nil {
			slog.Error(fmt.Sprintf("Failed to delete region info: %v", err))
		}

		// try to get an idle server to replace the down server if necessary
		if regionID != 0 && len(m.regions[regionID]) < regionServerNum && len(m.regions[0]) > 0 {
			newServer := m.regions[0][0]
			m.regions[0] = m.regions[0][1:]
			// update the server info
			if _, err := m.etcdClient.Put(m.etcdClient.Ctx(), serverPrefix+newServer, strconv.Itoa(regionID)); err != nil {
				slog.Error(fmt.Sprintf("Failed to update server info: %v", err))
			}
			// update the region info
			if _, err := m.etcdClient.Put(m.etcdClient.Ctx(), regionPrefix+strconv.Itoa(regionID)+"/"+newServer, strconv.Itoa(len(m.regions[regionID]))); err != nil {
				slog.Error(fmt.Sprintf("Failed to update region info: %v", err))
			}
			m.regions[regionID] = append(m.regions[regionID], newServer)
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
			// update the server info, move the servers to idle list
			for _, s := range m.regions[regionID] {
				if _, err := m.etcdClient.Put(m.etcdClient.Ctx(), serverPrefix+s, "0"); err != nil {
					slog.Error(fmt.Sprintf("Failed to update server info: %v", err))
				}
				m.regions[0] = append(m.regions[0], s)
			}
			delete(m.regions, regionID)
			delete(m.tableNum, regionID)
		}
	} else {
		slog.Warn(fmt.Sprintf("Table %s is not found", tableName))
	}
}

func (m *Master) getVisitTimes() {
	slog.Info("Getting visit times of each table...")
	m.visitNum = make(map[int]map[string]int)
	m.regionVisitNum = make(map[int]int)
	resp, err := m.etcdClient.Get(m.etcdClient.Ctx(), visitPrefix, clientv3.WithPrefix())
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to get visit info: %v", err))
		return
	}

	// get the visit times of each table in each region from etcd
	for _, kv := range resp.Kvs {
		tableName := string(kv.Key[len(visitPrefix):])
		times, err := strconv.Atoi(string(kv.Value))
		if err != nil {
			slog.Error(fmt.Sprintf("Failed to convert visit times: %v", err))
			continue
		}

		regionID := m.tables[tableName]
		if m.visitNum[regionID] == nil {
			m.visitNum[regionID] = make(map[string]int)
		}
		m.visitNum[regionID][tableName] = times
		m.regionVisitNum[regionID] += times
	}
	if len(m.regionVisitNum) == 0 {
		slog.Warn("No visit times found")
		return
	}

	minRegionID := 0
	maxRegionID := 0
	minVisitNum := math.MaxInt32
	maxVisitNum := 0
	visitAverage := 0.0
	for regionID, num := range m.regionVisitNum {
		visitAverage += float64(num)
		if num < minVisitNum {
			minVisitNum = num
			minRegionID = regionID
		}
		if num > maxVisitNum {
			maxVisitNum = num
			maxRegionID = regionID
		}
	}
	visitAverage /= float64(len(m.regionVisitNum))

	// migrate the tables from the hot region to the cold region
	if maxVisitNum > int(visitAverage*migrateThresholdHigh) && minVisitNum < int(visitAverage*migrateThresholdLow) {
		slog.Info(fmt.Sprintf("Region %d is hotest with %d visit times, region %d is coldest with %d visit times, average visit times: %f",
			maxRegionID, maxVisitNum, minRegionID, minVisitNum, visitAverage))
		// find an optimal table to migrate
		curDiff := m.regionVisitNum[maxRegionID] - m.regionVisitNum[minRegionID]
		tableName := ""
		minDiff := curDiff
		for name, times := range m.visitNum[maxRegionID] {
			if diff := int(math.Abs(float64(curDiff - times*2))); diff < minDiff {
				tableName = name
				minDiff = diff
			}
		}
		if tableName == "" {
			slog.Info("No table will be migrated because migrate any table will make the distribution worse")
			return
		}
		// migrate the table
		slog.Info(fmt.Sprintf("Migrate table %s with %d visit times from region %d to region %d", tableName, m.visitNum[maxRegionID][tableName], maxRegionID, minRegionID))

		// make a request to the region server to migrate the table
		postBody, err := json.Marshal(dto.MoveTableRequest{
			TableName:   tableName,
			Destination: m.regions[minRegionID][0],
		})
		if err != nil {
			slog.Error(fmt.Sprintf("Failed to marshal move table request: %v", err))
			return
		}
		resp, err := http.Post(requestWrapper(m.regions[maxRegionID][0], movePath), "application/json", strings.NewReader(string(postBody)))
		if err != nil {
			slog.Error(fmt.Sprintf("Failed to migrate table: %v", err))
			return
		}
		defer func(Body io.ReadCloser) {
			if err := Body.Close(); err != nil {
				slog.Error(fmt.Sprintf("Failed to close response body: %v", err))
			}
		}(resp.Body)
		if resp.StatusCode != http.StatusOK {
			slog.Error(fmt.Sprintf("Failed to migrate table: %s", resp.Status))
			return
		}
	} else {
		slog.Info(fmt.Sprintf("Current distribution is balanced, average visit times: %f", visitAverage))
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
		tables[len(tables)-1].IP = m.regions[regionID][0]
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
		if len(m.regions[0]) >= backupServerNum+regionServerNum {
			regionID = m.createNewRegion()
		} else {
			// find the region with the least visits
			minVisitNum := math.MaxInt32
			for id := range m.regions {
				if id == 0 {
					continue
				}
				// first compare the visit times, then the table number
				if visit, ok := m.regionVisitNum[id]; !ok && minVisitNum > 0 || visit < minVisitNum || visit == minVisitNum && m.tableNum[id] < m.tableNum[regionID] {
					if !ok {
						minVisitNum = 0
					} else {
						minVisitNum = visit
					}
					regionID = id
				}
			}
		}
		// if there is still no region
		if regionID == 0 {
			// first region only need `regionMinServerNum` servers
			if len(m.regions) == 1 && len(m.regions[0]) >= regionMinServerNum {
				regionID = m.createNewRegion()
			} else {
				return "", errors.New("no enough region servers")
			}
		}
	}
	if !m.checkRegionSafety(regionID) {
		return "", errors.New("no enough region servers")
	}
	return m.regions[regionID][0], nil
}

func (m *Master) createNewRegion() int {
	// get the new region servers
	var newRegionServers []string
	var serverNum = min(regionServerNum, len(m.regions[0]))
	for i := 0; i < serverNum; i++ {
		newRegionServers = append(newRegionServers, m.regions[0][0])
		m.regions[0] = m.regions[0][1:]
	}

	// create a new region
	// find the first empty region
	regionID := 1
	for ; regionID <= len(m.regions); regionID++ {
		if m.regions[regionID] == nil {
			break
		}
	}
	// append the new region servers to the new region
	m.regions[regionID] = make([]string, len(newRegionServers), regionServerNum)
	for i, ip := range newRegionServers {
		m.regions[regionID][i] = ip
		if _, err := m.etcdClient.Put(m.etcdClient.Ctx(), regionPrefix+strconv.Itoa(regionID)+"/"+ip, strconv.Itoa(i)); err != nil {
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
	return m.regions[regionID][0], nil
}

// ensure the region has at least `regionMinServerNum` servers
func (m *Master) checkRegionSafety(regionID int) bool {
	if len(m.regions[regionID]) == 0 {
		slog.Error(fmt.Sprintf("Region %d is not found", regionID))
		return false
	}

	for len(m.regions[regionID]) < regionMinServerNum {
		// try to assign idle servers to the region
		if m.regions[0] != nil && len(m.regions[0]) > 0 {
			newServer := m.regions[0][0]
			m.regions[0] = m.regions[0][1:]
			// update the server info
			if _, err := m.etcdClient.Put(m.etcdClient.Ctx(), serverPrefix+newServer, strconv.Itoa(regionID)); err != nil {
				slog.Error(fmt.Sprintf("Failed to update server info: %v", err))
				return false
			}
			// update the region info
			if _, err := m.etcdClient.Put(m.etcdClient.Ctx(), regionPrefix+strconv.Itoa(regionID)+"/"+newServer, strconv.Itoa(len(m.regions[regionID]))); err != nil {
				slog.Error(fmt.Sprintf("Failed to update region info: %v", err))
				return false
			}
			m.regions[regionID] = append(m.regions[regionID], newServer)
			slog.Info(fmt.Sprintf("Region server %s is assigned to region %d", newServer, regionID))
		} else {
			slog.Warn(fmt.Sprintf("Region %d has no enough servers", regionID))
			return false
		}
	}
	// if the region is not full and there are idle servers, assign them to the region
	for len(m.regions[regionID]) < regionServerNum && len(m.regions[0]) > 0 {
		newServer := m.regions[0][0]
		m.regions[0] = m.regions[0][1:]
		// update the server info
		if _, err := m.etcdClient.Put(m.etcdClient.Ctx(), serverPrefix+newServer, strconv.Itoa(regionID)); err != nil {
			slog.Error(fmt.Sprintf("Failed to update server info: %v", err))
			return false
		}
		// update the region info
		if _, err := m.etcdClient.Put(m.etcdClient.Ctx(), regionPrefix+strconv.Itoa(regionID)+"/"+newServer, strconv.Itoa(len(m.regions[regionID]))); err != nil {
			slog.Error(fmt.Sprintf("Failed to update region info: %v", err))
			return false
		}
		m.regions[regionID] = append(m.regions[regionID], newServer)
		slog.Info(fmt.Sprintf("Region server %s is assigned to region %d", newServer, regionID))
	}
	return true
}

func (m *Master) ShowTable() ([]string, error) {
	tables := make([]string, 0, len(m.tables))
	for tableName := range m.tables {
		tables = append(tables, tableName)
	}
	sort.Strings(tables)
	return tables, nil
}

func (m *Master) Stop() {
	slog.Info("Stopping master...")
	// shutdown the ticker
	m.ticker.Stop()

	// close the etcd client
	if err := m.etcdClient.Close(); err != nil {
		slog.Error(fmt.Sprintf("Failed to close etcd client: %v", err))
		return
	}

	// shutdown the server
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := m.server.Shutdown(ctx); err != nil {
		slog.Error(fmt.Sprintf("Failed to stop server: %v", err))
	}
}

func requestWrapper(ip string, path string) string {
	return "http://" + ip + ":8080" + path
}
