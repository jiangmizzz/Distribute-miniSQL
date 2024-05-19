package monitor

import (
	"fmt"
	"github.com/fatih/color"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/client/v3"
	"log/slog"
	"strconv"
	"strings"
	"time"
)

const (
	etcdEndpoints = "http://120.27.140.232:2379"
	serverPrefix  = "/server/" // etcd key prefix for server region ID
	regionPrefix  = "/region/" // etcd key prefix for new region
	tablePrefix   = "/table/"
	visitPrefix   = "/visit/"

	regionServerNum = 3 // the number of region servers in a region
)

type Monitor struct {
	etcdClient *clientv3.Client
	regions    map[int][]string    // regionID -> array of region servers, the first one is the primary server
	idleServer map[string]struct{} // idle region servers set
	tables     map[int][]string    // region ID -> array of table names
	visitNum   map[string]int      // table name -> visit times
}

func (m *Monitor) Start() {
	// connect to etcd
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{etcdEndpoints},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to connect to etcd: %s", color.RedString(err.Error())))
		return
	}
	m.etcdClient = cli

	// initialize member variables
	m.regions = make(map[int][]string)
	m.idleServer = make(map[string]struct{})
	m.tables = make(map[int][]string)
	m.visitNum = make(map[string]int)

	// watch server and region
	go m.watchServer()
	go m.watchRegion()
	go m.watchTable()
	go m.watchVisit()

	// get initial info from etcd
	m.getInitInfoFromEtcd()
}

func (m *Monitor) getInitInfoFromEtcd() {
	// get the server
	resp, err := m.etcdClient.Get(m.etcdClient.Ctx(), serverPrefix, clientv3.WithPrefix())
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to get server info: %s", color.RedString(err.Error())))
		return
	}
	for _, kv := range resp.Kvs {
		m.serverUp(kv)
	}

	// get the region
	resp, err = m.etcdClient.Get(m.etcdClient.Ctx(), regionPrefix, clientv3.WithPrefix())
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to get region info: %s", color.RedString(err.Error())))
		return
	}
	for _, kv := range resp.Kvs {
		m.regionUp(kv)
	}

	// get the table
	resp, err = m.etcdClient.Get(m.etcdClient.Ctx(), tablePrefix, clientv3.WithPrefix())
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to get table info: %s", color.RedString(err.Error())))
		return
	}
	for _, kv := range resp.Kvs {
		m.tableUp(kv)
	}

	// get the visit
	resp, err = m.etcdClient.Get(m.etcdClient.Ctx(), visitPrefix, clientv3.WithPrefix())
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to get visit info: %s", color.RedString(err.Error())))
		return
	}
	for _, kv := range resp.Kvs {
		m.visitUp(kv)
	}

	m.renderInfo()
}

func (m *Monitor) watchServer() {
	watcher := clientv3.NewWatcher(m.etcdClient)
	defer func(watcher clientv3.Watcher) {
		if err := watcher.Close(); err != nil {
			slog.Error(fmt.Sprintf("Failed to close watcher: %s", color.RedString(err.Error())))
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
					m.serverUp(event.Kv)
					m.renderInfo()
				case clientv3.EventTypeDelete:
					m.serverDown(event.PrevKv)
					m.renderInfo()
				}
			}
		}
	}
}

func (m *Monitor) serverUp(kv *mvccpb.KeyValue) {
	ip := string(kv.Key[len(serverPrefix):])
	regionID, err := strconv.Atoi(string(kv.Value))
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to convert region ID: %s", color.RedString(err.Error())))
		return
	}
	if regionID == 0 {
		m.idleServer[ip] = struct{}{}
	} else {
		delete(m.idleServer, ip)
	}
}

func (m *Monitor) serverDown(kv *mvccpb.KeyValue) {
	ip := string(kv.Key[len(serverPrefix):])
	delete(m.idleServer, ip)
}

func (m *Monitor) watchRegion() {
	watcher := clientv3.NewWatcher(m.etcdClient)
	defer func(watcher clientv3.Watcher) {
		if err := watcher.Close(); err != nil {
			slog.Error(fmt.Sprintf("Failed to close watcher: %s", color.RedString(err.Error())))
			return
		}
	}(watcher)

	watchChan := watcher.Watch(m.etcdClient.Ctx(), regionPrefix, clientv3.WithPrefix(), clientv3.WithPrevKV())
	for {
		select {
		case resp := <-watchChan:
			for _, event := range resp.Events {
				switch event.Type {
				case clientv3.EventTypePut:
					m.regionUp(event.Kv)
					m.renderInfo()
				case clientv3.EventTypeDelete:
					m.regionDown(event.PrevKv)
					m.renderInfo()
				}
			}
		}
	}
}

func (m *Monitor) parseRegion(kv *mvccpb.KeyValue) (int, string, int, error) {
	parts := strings.Split(string(kv.Key[len(regionPrefix):]), "/")
	if len(parts) != 2 {
		slog.Error(color.RedString("Invalid region info: %s", string(kv.Key)))
		return 0, "", 0, fmt.Errorf("invalid region info")
	}
	regionID, err := strconv.Atoi(parts[0])
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to convert region ID: %s", color.RedString(err.Error())))
		return 0, "", 0, err
	}
	ip := parts[1]
	index, err := strconv.Atoi(string(kv.Value))
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to convert master info: %s", color.RedString(err.Error())))
		return 0, "", 0, err
	}
	return regionID, ip, index, nil
}

func (m *Monitor) regionUp(kv *mvccpb.KeyValue) {
	regionID, ip, index, err := m.parseRegion(kv)
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to parse region info: %s", color.RedString(err.Error())))
		return
	}
	if _, ok := m.regions[regionID]; !ok {
		m.regions[regionID] = make([]string, regionServerNum)
	}
	m.regions[regionID][index] = ip
}

func (m *Monitor) regionDown(kv *mvccpb.KeyValue) {
	regionID, _, index, err := m.parseRegion(kv)
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to parse region info: %s", color.RedString(err.Error())))
		return
	}
	for i := index; i < regionServerNum-1; i++ {
		m.regions[regionID][i] = m.regions[regionID][i+1]
	}
	m.regions[regionID][regionServerNum-1] = ""
}

func (m *Monitor) watchTable() {
	watcher := clientv3.NewWatcher(m.etcdClient)
	defer func(watcher clientv3.Watcher) {
		if err := watcher.Close(); err != nil {
			slog.Error(fmt.Sprintf("Failed to close watcher: %s", color.RedString(err.Error())))
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
					m.tableUp(event.Kv)
					m.renderInfo()
				case clientv3.EventTypeDelete:
					m.tableDown(event.PrevKv)
					m.renderInfo()
				}
			}
		}
	}
}

func (m *Monitor) tableUp(kv *mvccpb.KeyValue) {
	tableName := string(kv.Key[len(tablePrefix):])
	regionID, err := strconv.Atoi(string(kv.Value))
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to convert region ID: %s", color.RedString(err.Error())))
		return
	}
	if _, ok := m.tables[regionID]; !ok {
		m.tables[regionID] = make([]string, 0)
	}
	m.tables[regionID] = append(m.tables[regionID], tableName)
}

func (m *Monitor) tableDown(kv *mvccpb.KeyValue) {
	tableName := string(kv.Key[len(tablePrefix):])
	regionID, err := strconv.Atoi(string(kv.Value))
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to convert region ID: %s", color.RedString(err.Error())))
		return
	}
	for i, name := range m.tables[regionID] {
		if name == tableName {
			m.tables[regionID] = append(m.tables[regionID][:i], m.tables[regionID][i+1:]...)
			break
		}
	}
}

func (m *Monitor) watchVisit() {
	watcher := clientv3.NewWatcher(m.etcdClient)
	defer func(watcher clientv3.Watcher) {
		if err := watcher.Close(); err != nil {
			slog.Error(fmt.Sprintf("Failed to close watcher: %s", color.RedString(err.Error())))
			return
		}
	}(watcher)

	watchChan := watcher.Watch(m.etcdClient.Ctx(), visitPrefix, clientv3.WithPrefix(), clientv3.WithPrevKV())
	for {
		select {
		case resp := <-watchChan:
			for _, event := range resp.Events {
				switch event.Type {
				case clientv3.EventTypePut:
					m.visitUp(event.Kv)
					m.renderInfo()
				case clientv3.EventTypeDelete:
					m.visitDown(event.PrevKv)
					m.renderInfo()
				}
			}
		}
	}
}

func (m *Monitor) visitUp(kv *mvccpb.KeyValue) {
	tableName := string(kv.Key[len(visitPrefix):])
	visit, err := strconv.Atoi(string(kv.Value))
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to convert visit times: %s", color.RedString(err.Error())))
		return
	}
	m.visitNum[tableName] = visit
}

func (m *Monitor) visitDown(kv *mvccpb.KeyValue) {
	tableName := string(kv.Key[len(visitPrefix):])
	delete(m.visitNum, tableName)
}

func (m *Monitor) renderInfo() {
	// clear the screen
	fmt.Print("\033[H\033[2J")

	// render region info
	fmt.Println(color.CyanString("[Region]"))
	for regionID, servers := range m.regions {
		fmt.Printf(color.BlueString("Region %d:\t", regionID))
		for i, server := range servers {
			if i == 0 {
				fmt.Printf(color.GreenString("%s", server))
			} else {
				fmt.Printf(color.YellowString("\t%s", server))
			}
		}
		fmt.Println()
	}
	fmt.Println()
	// render idle server info
	fmt.Println(color.CyanString("[Idle Server]"))
	for ip := range m.idleServer {
		fmt.Printf("%s\t", color.GreenString(ip))
	}
	fmt.Println()
	fmt.Println()
	// render table info
	fmt.Println(color.CyanString("[Table]"))
	for regionID, _ := range m.regions {
		fmt.Printf(color.BlueString("Region %d:\t", regionID))
		for _, table := range m.tables[regionID] {
			fmt.Printf("%s(%d)\t", color.MagentaString("%s", table), m.visitNum[table])
		}
		fmt.Println()
	}
}

func (m *Monitor) Stop() {
	// close the etcd client
	if err := m.etcdClient.Close(); err != nil {
		slog.Error(fmt.Sprintf("Failed to close etcd client: %s", color.RedString(err.Error())))
		return
	}
}
