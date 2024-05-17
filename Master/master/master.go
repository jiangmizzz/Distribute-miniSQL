package master

import (
	"Master/api/dto"
	"Master/controller"
	"Master/route"
	. "Master/utils"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/fatih/color"
	"github.com/gin-gonic/gin"
	"go.etcd.io/etcd/client/v3"
	"io"
	"log/slog"
	"math"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	etcdEndpoints           = "http://localhost:2379"
	availableRegionIdPrefix = "/availableRegionId/"
	discoverPrefix          = "/server/discovery/" // etcd key prefix for discover new server
	serverPrefix            = "/server/"           // etcd key prefix for server region ID
	regionPrefix            = "/region/"           // etcd key prefix for new region
	tablePrefix             = "/table/"
	visitPrefix             = "/visit/"

	movePath = "/table/move"
	syncPath = "/node/sync"

	regionServerNum    = 3   // the number of region servers in a region
	backupServerNum    = 2   // the number of idle servers for backup
	regionMinServerNum = 2   // the minimum number of region servers in a region (first region)
	loadBalanceCycle   = 5   // the cycle of load balance (minutes)
	moveThresholdHigh  = 1.2 // if a region has more than 20% of the average visit times, it is considered as a hot region
	moveThresholdLow   = 0.8 // if a region has less than 20% of the average visit times, it is considered as a cold region
)

var log *slog.Logger

type Master struct {
	controller        controller.Controller
	server            *http.Server
	etcdClient        *clientv3.Client
	ticker            *time.Ticker
	regions           map[int][]string       // regionID -> array of region servers, the first one is the primary server
	servers           map[string]struct{}    // all region servers set
	tables            map[string]int         // table name -> region ID
	tableNum          map[int]int            // region ID -> table number
	visitNum          map[int]map[string]int // region ID -> table name -> visit times
	regionVisitNum    map[int]int            // region ID -> total visit times
	availableRegionID int                    // next available region ID
}

func (m *Master) Start() {
	opts := slog.HandlerOptions{
		Level: slog.LevelInfo,
	}
	handler := NewPrettyHandler(os.Stdout, opts)
	log = slog.New(handler)

	log.Info("Master is starting...")
	// connect to etcd
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{etcdEndpoints},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Error(fmt.Sprintf("Failed to connect to etcd: %s", color.RedString(err.Error())))
		return
	}
	m.etcdClient = cli

	// initialize member variables
	m.controller = controller.Controller{Service: m}
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
		log.Info(fmt.Sprintf("Server is running on %s", color.BlueString(m.server.Addr)))
		if err = m.server.ListenAndServe(); err != nil && !errors.Is(http.ErrServerClosed, err) {
			log.Error(fmt.Sprintf("Failed to run server: %s", color.RedString(err.Error())))
		}
	}()

	// get the server already in etcd
	m.getInitInfoFromEtcd()

	// set ticker to get the visit times of each table
	m.ticker = time.NewTicker(loadBalanceCycle * time.Minute)
	go func() {
		for range m.ticker.C {
			m.getVisitTimes()
			m.loadBalance()
		}
	}()

	log.Info("Master initialized " + color.GreenString("successfully"))
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
	// get the next available region ID
	resp, err := m.etcdClient.Get(m.etcdClient.Ctx(), availableRegionIdPrefix)
	if err != nil {
		log.Error(fmt.Sprintf("Failed to get available region ID: %s", color.RedString(err.Error())))
		return
	}
	if len(resp.Kvs) == 0 {
		m.availableRegionID = 1
		_, err = m.etcdClient.Put(m.etcdClient.Ctx(), availableRegionIdPrefix, "1")
		if err != nil {
			log.Error(fmt.Sprintf("Failed to update available region ID: %s", color.RedString(err.Error())))
			return
		}
	} else if m.availableRegionID, err = strconv.Atoi(string(resp.Kvs[0].Value)); err != nil {
		log.Error(fmt.Sprintf("Failed to convert available region ID: %s", color.RedString(err.Error())))
		return
	}

	// get the region info (index in the region)
	serverIndex := make(map[int]map[string]int) // regionID -> server IP -> index in the region
	resp, err = m.etcdClient.Get(m.etcdClient.Ctx(), regionPrefix, clientv3.WithPrefix())
	if err != nil {
		log.Error(fmt.Sprintf("Failed to get region info: %s", color.RedString(err.Error())))
		return
	}
	for _, kv := range resp.Kvs {
		parts := strings.Split(string(kv.Key[len(regionPrefix):]), "/")
		if len(parts) != 2 {
			log.Error(color.RedString("Invalid region info: %s", string(kv.Key)))
			continue
		}
		regionID, err := strconv.Atoi(parts[0])
		if err != nil {
			log.Error(fmt.Sprintf("Failed to convert region ID: %s", color.RedString(err.Error())))
			continue
		}
		ip := parts[1]

		index, err := strconv.Atoi(string(kv.Value))
		if err != nil {
			log.Error(fmt.Sprintf("Failed to convert master info: %s", color.RedString(err.Error())))
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
		log.Error(fmt.Sprintf("Failed to get server info: %s", color.RedString(err.Error())))
		return
	}
	for _, kv := range resp.Kvs {
		ip := string(kv.Key[len(serverPrefix):])
		regionID, err := strconv.Atoi(string(kv.Value))
		if err != nil {
			log.Error(fmt.Sprintf("Failed to convert region ID: %s", color.RedString(err.Error())))
			continue
		}
		serverRegion[ip] = regionID
	}
	// then get all discovered server info
	resp, err = m.etcdClient.Get(m.etcdClient.Ctx(), discoverPrefix, clientv3.WithPrefix())
	if err != nil {
		log.Error(fmt.Sprintf("Failed to get discovered server info: %s", color.RedString(err.Error())))
		return
	}
	for _, kv := range resp.Kvs {
		ip := string(kv.Key[len(discoverPrefix):])
		regionID, err := strconv.Atoi(string(kv.Value))
		if err != nil {
			log.Error(fmt.Sprintf("Failed to convert region ID: %s", color.RedString(err.Error())))
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
		log.Error(fmt.Sprintf("Failed to get table info: %s", color.RedString(err.Error())))
		return
	}
	for _, kv := range resp.Kvs {
		tableName := string(kv.Key[len(tablePrefix):])
		regionID, err := strconv.Atoi(string(kv.Value))
		if err != nil {
			log.Error(fmt.Sprintf("Failed to convert region ID: %s", color.RedString(err.Error())))
			continue
		}
		m.tableCreate(tableName, regionID)
	}

	// get the visit times of each table
	m.getVisitTimes()
}

func (m *Master) watchRegionServer() {
	log.Info("Watching for new region server...")
	watcher := clientv3.NewWatcher(m.etcdClient)
	defer func(watcher clientv3.Watcher) {
		err := watcher.Close()
		if err != nil {
			log.Error(fmt.Sprintf("Failed to close etcd watcher: %s", color.RedString(err.Error())))
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
						log.Error(fmt.Sprintf("Failed to convert region ID: %s", color.RedString(err.Error())))
						continue
					}
					m.serverUp(ip, regionID)
				case clientv3.EventTypeDelete:
					ip := string(event.PrevKv.Key[len(discoverPrefix):])
					resp, err := m.etcdClient.Get(m.etcdClient.Ctx(), serverPrefix+ip)
					if err != nil {
						log.Error(fmt.Sprintf("Failed to get server info: %s", color.RedString(err.Error())))
						continue
					}
					regionID, err := strconv.Atoi(string(resp.Kvs[0].Value))
					if err != nil {
						log.Error(fmt.Sprintf("Failed to convert region ID: %s", color.RedString(err.Error())))
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
	log.Info(fmt.Sprintf("Region server is already %s: %s", color.GreenString("up"), color.BlueString(ip)))
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

	// update server info
	if _, err := m.etcdClient.Put(m.etcdClient.Ctx(), serverPrefix+ip, strconv.Itoa(regionID)); err != nil {
		log.Error(fmt.Sprintf("Failed to update server info: %s", color.RedString(err.Error())))
	}
}

func (m *Master) serverUp(ip string, regionID int) {
	// check if the server is already up
	if _, ok := m.servers[ip]; ok {
		return
	}
	log.Info(fmt.Sprintf("Region server %s: %s", color.GreenString("up"), color.BlueString(ip)))
	m.servers[ip] = struct{}{}

	// if the region is full, assign the server to the idle list
	if regionID != 0 && len(m.regions[regionID]) >= regionServerNum {
		if _, err := m.etcdClient.Put(m.etcdClient.Ctx(), serverPrefix+ip, "0"); err != nil {
			log.Error(fmt.Sprintf("Failed to update server info: %s", color.RedString(err.Error())))
		}
		m.regions[0] = append(m.regions[0], ip)
		log.Info(fmt.Sprintf("Server %s is reassigned:\t%s --> %s", color.BlueString(ip), color.YellowString("%d", regionID), color.GreenString("0")))
		return
	} else {
		if regionID != 0 && len(m.regions[regionID]) > 0 {
			m.requestSync(ip, m.regions[regionID][0])
		}

		if _, err := m.etcdClient.Put(m.etcdClient.Ctx(), serverPrefix+ip, strconv.Itoa(regionID)); err != nil {
			log.Error(fmt.Sprintf("Failed to update server info: %s", color.RedString(err.Error())))
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

	// update available region ID if necessary
	if regionID >= m.availableRegionID {
		m.availableRegionID = regionID + 1
		_, err := m.etcdClient.Put(m.etcdClient.Ctx(), availableRegionIdPrefix, strconv.Itoa(m.availableRegionID))
		if err != nil {
			log.Error(fmt.Sprintf("Failed to update available region ID: %s", color.RedString(err.Error())))
		}
	}

	if regionID != 0 {
		// update the server info if the server in a region
		_, err := m.etcdClient.Put(m.etcdClient.Ctx(), regionPrefix+strconv.Itoa(regionID)+"/"+ip, strconv.Itoa(len(m.regions[regionID])-1))
		if err != nil {
			log.Error(fmt.Sprintf("Failed to update region info: %s", color.RedString(err.Error())))
		}
	}
}

func (m *Master) serverDown(ip string, regionID int) {
	// check if the server is already down
	if _, ok := m.servers[ip]; !ok {
		return
	}
	log.Info(fmt.Sprintf("Region server %s: %s", color.RedString("down"), color.BlueString(ip)))
	// delete server info
	if _, err := m.etcdClient.Delete(m.etcdClient.Ctx(), serverPrefix+ip); err != nil {
		log.Error(fmt.Sprintf("Failed to delete server info: %s", color.RedString(err.Error())))
	}

	if m.regions[regionID] != nil {
		for i, s := range m.regions[regionID] {
			if s == ip {
				if regionID != 0 && i == 0 && len(m.regions[regionID]) > 1 {
					log.Info(fmt.Sprintf("Master server of region %s changed:\t%s --> %s", color.BlueString(strconv.Itoa(regionID)), color.RedString(ip), color.GreenString(m.regions[regionID][1])))
				}
				// update successor server's index
				if regionID != 0 {
					for j := i + 1; j < len(m.regions[regionID]); j++ {
						if _, err := m.etcdClient.Put(m.etcdClient.Ctx(), regionPrefix+strconv.Itoa(regionID)+"/"+m.regions[regionID][j], strconv.Itoa(j-1)); err != nil {
							log.Error(fmt.Sprintf("Failed to update region info: %s", color.RedString(err.Error())))
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
		if regionID != 0 && len(m.regions[regionID]) == 0 {
			log.Info(fmt.Sprintf("Region %s is %s", color.YellowString(strconv.Itoa(regionID)), color.RedString("empty")))
			delete(m.regions, regionID)
		}
		// delete the server info
		if _, err := m.etcdClient.Delete(m.etcdClient.Ctx(), regionPrefix+strconv.Itoa(regionID)+"/"+ip); err != nil {
			log.Error(fmt.Sprintf("Failed to delete region info: %s", color.RedString(err.Error())))
		}

		// try to get an idle server to replace the down server if necessary
		if regionID != 0 && len(m.regions[regionID]) > 0 && len(m.regions[regionID]) < regionServerNum && len(m.regions[0]) > 0 {
			newServer := m.regions[0][0]
			m.regions[0] = m.regions[0][1:]
			// update the server info
			if _, err := m.etcdClient.Put(m.etcdClient.Ctx(), serverPrefix+newServer, strconv.Itoa(regionID)); err != nil {
				log.Error(fmt.Sprintf("Failed to update server info: %s", color.RedString(err.Error())))
			}
			// update the region info
			if _, err := m.etcdClient.Put(m.etcdClient.Ctx(), regionPrefix+strconv.Itoa(regionID)+"/"+newServer, strconv.Itoa(len(m.regions[regionID]))); err != nil {
				log.Error(fmt.Sprintf("Failed to update region info: %s", color.RedString(err.Error())))
			}
			m.regions[regionID] = append(m.regions[regionID], newServer)
			m.requestSync(newServer, m.regions[regionID][0])
			log.Info(fmt.Sprintf("Server %s is reassigned:\t%s --> %s", color.BlueString(newServer), color.YellowString("0"), color.GreenString("%d", regionID)))
		}
	} else {
		log.Error(color.RedString("Region %s is not found", regionID))
	}
}

func (m *Master) requestSync(ip string, master string) {
	// make a request to the region server to sync the data
	log.Info(fmt.Sprintf("Requesting region server %s to sync data", color.BlueString(ip)))
	postBody, err := json.Marshal(dto.SyncDataRequest{
		Destination: master,
	})
	if err != nil {
		log.Error(fmt.Sprintf("Failed to marshal sync data request: %s", color.RedString(err.Error())))
		return
	}
	resp, err := http.Post(requestWrapper(ip, syncPath), "application/json", strings.NewReader(string(postBody)))
	if err != nil {
		log.Error(fmt.Sprintf("Failed to sync data: %s", color.RedString(err.Error())))
		return
	}
	defer func(Body io.ReadCloser) {
		if err := Body.Close(); err != nil {
			log.Error(fmt.Sprintf("Failed to close response body: %s", color.RedString(err.Error())))
		}
	}(resp.Body)
	if resp.StatusCode != http.StatusOK {
		log.Error(fmt.Sprintf("Failed to sync data: %s", color.RedString(resp.Status)))
		return
	}
}

func (m *Master) watchTable() {
	log.Info("Watching for new table...")
	watcher := clientv3.NewWatcher(m.etcdClient)
	defer func(watcher clientv3.Watcher) {
		err := watcher.Close()
		if err != nil {
			log.Error(fmt.Sprintf("Failed to close etcd watcher: %s", color.RedString(err.Error())))
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
						log.Error(fmt.Sprintf("Failed to convert region ID: %s", color.RedString(err.Error())))
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
	if oldID, ok := m.tables[tableName]; !ok {
		log.Info(fmt.Sprintf("Table %s is created in region %s", color.GreenString(tableName), color.BlueString(strconv.Itoa(regionID))))
		m.tableNum[regionID]++
	} else {
		log.Info(fmt.Sprintf("Table %s is moved:\t%s --> %s", color.BlueString(tableName), color.YellowString("%d", oldID), color.GreenString("%d", regionID)))
	}
	m.tables[tableName] = regionID
}

func (m *Master) tableDelete(tableName string) {
	if regionID, ok := m.tables[tableName]; ok {
		log.Info(fmt.Sprintf("Table %s is removed from region %s", color.RedString(tableName), color.BlueString(strconv.Itoa(regionID))))
		m.tableNum[regionID]--
		delete(m.tables, tableName)
		// if the region is empty, remove it
		if m.tableNum[regionID] == 0 {
			// delete the region info
			if _, err := m.etcdClient.Delete(m.etcdClient.Ctx(), regionPrefix+strconv.Itoa(regionID), clientv3.WithPrefix()); err != nil {
				log.Error(fmt.Sprintf("Failed to delete region info: %s", color.RedString(err.Error())))
			}

			// check if the region is existed
			if regionID == 0 {
				log.Warn(color.YellowString("Region 0 is not valid"))
				return
			}
			if _, ok := m.regions[regionID]; !ok {
				log.Warn(fmt.Sprintf("Region %s is not found", color.YellowString("%d", regionID)))
				return
			}
			// update the server info, move the servers to idle list
			for _, s := range m.regions[regionID] {
				if _, err := m.etcdClient.Put(m.etcdClient.Ctx(), serverPrefix+s, "0"); err != nil {
					log.Error(fmt.Sprintf("Failed to update server info: %s", color.RedString(err.Error())))
				}
				m.regions[0] = append(m.regions[0], s)
			}
			delete(m.regions, regionID)
			delete(m.tableNum, regionID)
		}
	} else {
		log.Warn(fmt.Sprintf("Table %s is not found", color.YellowString(tableName)))
	}
}

func (m *Master) getVisitTimes() {
	log.Info("Getting visit times of each table...")
	m.visitNum = make(map[int]map[string]int)
	m.regionVisitNum = make(map[int]int)
	resp, err := m.etcdClient.Get(m.etcdClient.Ctx(), visitPrefix, clientv3.WithPrefix())
	if err != nil {
		log.Error(fmt.Sprintf("Failed to get visit info: %s", color.RedString(err.Error())))
		return
	}

	// get the visit times of each table in each region from etcd
	for _, kv := range resp.Kvs {
		tableName := string(kv.Key[len(visitPrefix):])
		times, err := strconv.Atoi(string(kv.Value))
		if err != nil {
			log.Error(fmt.Sprintf("Failed to convert visit times: %s", color.RedString(err.Error())))
			continue
		}

		regionID := m.tables[tableName]
		if len(m.regions[regionID]) == 0 {
			continue
		}
		if m.visitNum[regionID] == nil {
			m.visitNum[regionID] = make(map[string]int)
		}
		m.visitNum[regionID][tableName] = times
		m.regionVisitNum[regionID] += times

		// delete the visit info
		if _, err := m.etcdClient.Delete(m.etcdClient.Ctx(), visitPrefix+tableName); err != nil {
			log.Error(fmt.Sprintf("Failed to delete visit info: %s", color.RedString(err.Error())))
		}
	}
}

func (m *Master) loadBalance() {
	if len(m.regionVisitNum) == 0 {
		log.Warn(color.YellowString("No visit times found"))
		return
	}

	minRegionID := 0
	maxRegionID := 0
	minVisitNum := math.MaxInt32
	maxVisitNum := 0
	visitAverage := 0.0
	for regionID, num := range m.regionVisitNum {
		if len(m.regions[regionID]) == 0 {
			continue
		}
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

	// move the tables from the hot region to the cold region
	if maxVisitNum > int(visitAverage*moveThresholdHigh) && minVisitNum < int(visitAverage*moveThresholdLow) {
		log.Info(fmt.Sprintf("Hotest %s: %s times\tColdest %s: %s times\tAverage: %s times", color.RedString("%d", maxRegionID), color.RedString("%d", maxVisitNum), color.GreenString("%d", minRegionID), color.GreenString("%d", minVisitNum), color.YellowString("%f", visitAverage)))
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
			log.Info("No table will be moved because move any table will make the distribution worse")
			return
		}
		// move the table
		log.Info(fmt.Sprintf("Table %s: %stimes is moved:\t%s --> %s", color.BlueString(tableName), color.BlueString("%d", m.visitNum[maxRegionID][tableName]), color.RedString("%d", maxRegionID), color.GreenString("%d", minRegionID)))

		// make a request to the region server to move the table
		postBody, err := json.Marshal(dto.MoveTableRequest{
			TableName:   tableName,
			Destination: m.regions[minRegionID][0],
		})
		if err != nil {
			log.Error(fmt.Sprintf("Failed to marshal move table request: %s", color.RedString(err.Error())))
			return
		}
		resp, err := http.Post(requestWrapper(m.regions[maxRegionID][0], movePath), "application/json", strings.NewReader(string(postBody)))
		if err != nil {
			log.Error(fmt.Sprintf("Failed to move table: %s", color.RedString(err.Error())))
			return
		}
		defer func(Body io.ReadCloser) {
			if err := Body.Close(); err != nil {
				log.Error(fmt.Sprintf("Failed to close response body: %s", color.RedString(err.Error())))
			}
		}(resp.Body)
		if resp.StatusCode != http.StatusOK {
			log.Error(fmt.Sprintf("Failed to move table: %s", color.RedString(resp.Status)))
			return
		}
	} else {
		log.Info(fmt.Sprintf("Current distribution is balanced, average visit times: %s", color.YellowString("%f", visitAverage)))
	}
}

func (m *Master) QueryTable(tableNames []string) ([]dto.QueryTableResponse, error) {
	var tables []dto.QueryTableResponse
	for _, tableName := range tableNames {
		// get the region stored the table
		resp, err := m.etcdClient.Get(m.etcdClient.Ctx(), tablePrefix+tableName)
		if err != nil {
			log.Error(fmt.Sprintf("Failed to get table info: %s", color.RedString(err.Error())))
			return nil, err
		}
		tables = append(tables, dto.QueryTableResponse{
			Name: tableName,
			IP:   "",
		})

		// get the master server of the region if exist
		if resp.Count == 0 {
			log.Warn(fmt.Sprintf("Table %s is not found.", color.RedString(tableName)))
			continue
		}
		regionID, err := strconv.Atoi(string(resp.Kvs[0].Value[0]))
		if err != nil {
			log.Error(fmt.Sprintf("Failed to convert region ID: %s", color.RedString(err.Error())))
			return nil, err
		}
		if len(m.regions[regionID]) == 0 || !m.checkRegionSafety(regionID) {
			continue
		}
		tables[len(tables)-1].IP = m.regions[regionID][0]
		log.Info(fmt.Sprintf("Table %s is found in region %s: %s", color.GreenString(tableName), color.GreenString("%d", regionID), color.BlueString(m.regions[regionID][0])))
	}
	return tables, nil
}

func (m *Master) NewTable(tableName string) (string, error) {
	// check if the table is already exist
	if _, ok := m.tables[tableName]; ok {
		log.Warn(fmt.Sprintf("Table %s is already exist.", color.RedString(tableName)))
		return "", errors.New("table already exist")
	}

	// find the region to store the table
	regionID := 0
	// find if there is a region with no table
	for id, num := range m.tableNum {
		if num == 0 && len(m.regions[id]) > 0 {
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
				if id == 0 || len(m.regions[id]) == 0 {
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
	log.Info(fmt.Sprintf("Table %s is created in region %s: %s", color.GreenString(tableName), color.GreenString("%d", regionID), color.BlueString(m.regions[regionID][0])))
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
	regionID := m.availableRegionID
	m.availableRegionID++
	if _, err := m.etcdClient.Put(m.etcdClient.Ctx(), availableRegionIdPrefix, strconv.Itoa(m.availableRegionID)); err != nil {
		log.Error(fmt.Sprintf("Failed to update available region ID: %s", color.RedString(err.Error())))
	}
	// append the new region servers to the new region
	m.regions[regionID] = make([]string, len(newRegionServers), regionServerNum)
	for i, ip := range newRegionServers {
		m.regions[regionID][i] = ip
		if _, err := m.etcdClient.Put(m.etcdClient.Ctx(), regionPrefix+strconv.Itoa(regionID)+"/"+ip, strconv.Itoa(i)); err != nil {
			log.Error(fmt.Sprintf("Failed to update region info: %s", color.RedString(err.Error())))
		}
	}
	for _, ip := range newRegionServers {
		if _, err := m.etcdClient.Put(m.etcdClient.Ctx(), serverPrefix+ip, strconv.Itoa(regionID)); err != nil {
			log.Error(fmt.Sprintf("Failed to update server info: %s", color.RedString(err.Error())))
		}
	}
	m.tableNum[regionID] = 0
	log.Info(fmt.Sprintf("New region %s is created: %s", color.GreenString("%d", regionID), color.BlueString("%v", newRegionServers)))
	return regionID
}

func (m *Master) DeleteTable(tableName string) (string, error) {
	// check if the table exists
	if _, ok := m.tables[tableName]; !ok {
		log.Warn(fmt.Sprintf("Table %s is not exist.", color.RedString(tableName)))
		return "", errors.New("table not exist")
	}

	// get the region stored the table
	regionID := m.tables[tableName]
	if len(m.regions[regionID]) == 0 || !m.checkRegionSafety(regionID) {
		return "", errors.New("no enough region servers")
	}
	log.Info(fmt.Sprintf("Table %s is removed from region %s: %s", color.RedString(tableName), color.GreenString("%d", regionID), color.BlueString(m.regions[regionID][0])))
	return m.regions[regionID][0], nil
}

// ensure the region has at least `regionMinServerNum` servers
func (m *Master) checkRegionSafety(regionID int) bool {
	if len(m.regions[regionID]) == 0 {
		log.Error(fmt.Sprintf("Region %s is not found", color.RedString("%d", regionID)))
		return false
	}

	for len(m.regions[regionID]) < regionMinServerNum {
		// try to assign idle servers to the region
		if len(m.regions[0]) > 0 {
			newServer := m.regions[0][0]
			m.regions[0] = m.regions[0][1:]
			// update the server info
			if _, err := m.etcdClient.Put(m.etcdClient.Ctx(), serverPrefix+newServer, strconv.Itoa(regionID)); err != nil {
				log.Error(fmt.Sprintf("Failed to update server info: %s", color.RedString(err.Error())))
				return false
			}
			// update the region info
			if _, err := m.etcdClient.Put(m.etcdClient.Ctx(), regionPrefix+strconv.Itoa(regionID)+"/"+newServer, strconv.Itoa(len(m.regions[regionID]))); err != nil {
				log.Error(fmt.Sprintf("Failed to update region info: %s", color.RedString(err.Error())))
				return false
			}
			m.regions[regionID] = append(m.regions[regionID], newServer)
			m.requestSync(newServer, m.regions[regionID][0])
			log.Info(fmt.Sprintf("Server %s is reassigned:\t%s --> %s", color.BlueString(newServer), color.YellowString("0"), color.GreenString("%d", regionID)))
		} else {
			log.Warn(fmt.Sprintf("Region %s has no enough servers", color.YellowString("%d", regionID)))
			return false
		}
	}
	// if the region is not full and there are idle servers, assign them to the region
	for len(m.regions[regionID]) < regionServerNum && len(m.regions[0]) > 0 {
		newServer := m.regions[0][0]
		m.regions[0] = m.regions[0][1:]
		// update the server info
		if _, err := m.etcdClient.Put(m.etcdClient.Ctx(), serverPrefix+newServer, strconv.Itoa(regionID)); err != nil {
			log.Error(fmt.Sprintf("Failed to update server info: %s", color.RedString(err.Error())))
			return false
		}
		// update the region info
		if _, err := m.etcdClient.Put(m.etcdClient.Ctx(), regionPrefix+strconv.Itoa(regionID)+"/"+newServer, strconv.Itoa(len(m.regions[regionID]))); err != nil {
			log.Error(fmt.Sprintf("Failed to update region info: %s", color.RedString(err.Error())))
			return false
		}
		m.regions[regionID] = append(m.regions[regionID], newServer)
		log.Info(fmt.Sprintf("Server %s is reassigned:\t%s --> %s", color.BlueString(newServer), color.YellowString("0"), color.GreenString("%d", regionID)))
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
	log.Info("Stopping master...")
	// shutdown the ticker
	m.ticker.Stop()

	// close the etcd client
	if err := m.etcdClient.Close(); err != nil {
		log.Error(fmt.Sprintf("Failed to close etcd client: %s", color.RedString(err.Error())))
		return
	}

	// shutdown the server
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := m.server.Shutdown(ctx); err != nil {
		log.Error(fmt.Sprintf("Failed to stop server: %s", color.RedString(err.Error())))
	}
}

func requestWrapper(ip string, path string) string {
	return "http://" + ip + ":8080" + path
}
