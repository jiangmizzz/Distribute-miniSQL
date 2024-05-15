package controller

import (
	"Region/api/dto"
	"Region/database"
	"Region/server"
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os/exec"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go-sql-driver/mysql"
	"github.com/spf13/viper"
)

// 消息队列 当执行表迁移时开启，用于记录增量更新的 sql 语句
var messageQueue = MessageQueue{
	Statements: []string{},
	Activate:   false,
	Mutex:      &sync.RWMutex{},
}

// 用sql指令进行数据查询
type SqlStatement struct {
	ReqId     string `json:"reqId"`
	TableName string `json:"tableName"`
	Statement string `json:"statement"`
}

// 在处理请求前：1. 检查 master/slave 状态和请求所属状态是否对应 2. 参数是否有误 3. reqId 是否已执行

// QueryHandler 用sql指令进行数据查询
func QueryHandler(c *gin.Context) {
	// 状态是否为 master
	if checkStatus(c) {
		return
	}
	//判断参数是否有误
	stmt := SqlStatement{}
	if err := c.BindJSON(&stmt); err != nil {
		// 参数有误
		c.JSON(http.StatusBadRequest, dto.ResponseType[string]{
			Success: false,
			ErrCode: "400",
			ErrMsg:  "parameters error",
			Data:    "null",
		})
		return
	}

	// 开始查询, 还需要返回字段名
	rows, err := database.Mysql.Query(stmt.Statement)
	if err != nil {
		var driverError *mysql.MySQLError
		if errors.As(err, &driverError) {
			fmt.Printf("Query error! err: %v\n", driverError)
			if driverError.Number == 1146 { // 表不存在
				c.JSON(http.StatusBadRequest, dto.ResponseType[string]{
					Success: false,
					ErrCode: "1146",
					ErrMsg:  "Table not exist",
					Data:    "null",
				})
				return
			}
		}
		// 访问量+1
		updateVisits(stmt.TableName, 1)
		c.JSON(http.StatusInternalServerError, dto.ResponseType[string]{
			Success: false,
			ErrCode: "500",
			ErrMsg:  "Query error",
			Data:    "null",
		})
		return
	}
	defer rows.Close()
	//返回列名和各行的值
	cols, colErr := rows.Columns()
	if colErr != nil {
		c.JSON(http.StatusInternalServerError, dto.ResponseType[string]{
			Success: false,
			ErrCode: "500",
			ErrMsg:  "Query error: fail to get columns!",
			Data:    "null",
		})
		return
	}
	var results [][]string
	// values：存储每一行原始字节切片形式的值
	values := make([]interface{}, len(cols))
	for i := range values {
		var s sql.RawBytes
		values[i] = &s
	}
	// 扫描每行，将结果添加到 results 中
	for rows.Next() {
		if err := rows.Scan(values...); err != nil {
			c.JSON(http.StatusInternalServerError, dto.ResponseType[string]{
				Success: false,
				ErrCode: "500",
				ErrMsg:  "Query error: fail to scan row!",
				Data:    "null",
			})
			return
		}
		var row []string //单行 string 值
		for _, value := range values {
			// 将字段值转换为字符串并存储到结果切片中
			row = append(row, string(*(value.(*sql.RawBytes))))
		}
		// 将每行的结果添加到结果集中
		results = append(results, row)
	}
	c.JSON(http.StatusInternalServerError, dto.ResponseType[QueryResponse]{
		Success: false,
		ErrCode: "500",
		ErrMsg:  "Query error: fail to scan row!",
		Data: QueryResponse{
			Cols: cols, Rows: results,
		},
	})
	return
}

// WriteHandler sql写操作
func WriteHandler(c *gin.Context) {
	// 状态是否为 master
	if checkStatus(c) {
		return
	}
	// 判断参数是否有误
	stmt := SqlStatement{}
	if err := c.BindJSON(&stmt); err != nil {
		// 参数有误
		c.JSON(http.StatusBadRequest, dto.ResponseType[string]{
			Success: false,
			ErrCode: "400",
			ErrMsg:  "parameters error",
			Data:    "null",
		})
		return
	}
	// 判断该 req 是否已经执行过
	if checkReq(c, stmt.ReqId) {
		return
	}
	// 开启事务并执行操作
	txn, _ := database.Mysql.Begin()
	_, exeErr := txn.Exec(stmt.Statement)
	if exeErr != nil {
		// sql执行有误
		var driverError *mysql.MySQLError
		if errors.As(exeErr, &driverError) {
			fmt.Printf("Execution error! err: %v\n", driverError)
			if driverError.Number == 1146 { // 表不存在
				c.JSON(http.StatusBadRequest, dto.ResponseType[string]{
					Success: false,
					ErrCode: "1146",
					ErrMsg:  "Table not exist",
					Data:    "null",
				})
				return
			}
		}
		//更新访问量 (2)
		updateVisits(stmt.TableName, 2)
		_ = txn.Rollback()
		c.JSON(http.StatusInternalServerError, dto.ResponseType[string]{
			Success: false,
			ErrCode: "500",
			ErrMsg:  "Execution error",
			Data:    "null",
		})
		return
	}
	// 执行成功，获取 slave IP，准备同步
	fmt.Println("Execution success!")
	slaves := server.Rs.GetNodes()
	updateVisits(stmt.TableName, 2*(len(slaves)+1)) // 更新访问量(+2*len(slaves))
	// 表同步
	syncRes := tableSync(slaves, stmt)
	if syncRes {
		// slave 均无误后，自己提交txn
		_ = txn.Commit()
		reqQueue.Add(stmt.ReqId) // 添加 reqId
		c.JSON(http.StatusOK, dto.ResponseType[string]{
			Success: true,
			ErrCode: "200",
			ErrMsg:  "Write successfully",
			Data:    "null",
		})
	} else {
		//同步有误，回滚操作
		_ = txn.Rollback()
		c.JSON(http.StatusInternalServerError, dto.ResponseType[string]{
			Success: false,
			ErrCode: "500",
			ErrMsg:  "Sync error",
			Data:    "null",
		})
	}
	// 向 slave 同步 commit 信号
	for ip := range slaves {
		url := fmt.Sprintf("http://%s:%s/api/table/commit", ip, "8080")
		data := make(map[string]interface{})
		data["reqId"] = stmt.ReqId
		data["isCommit"] = syncRes
		bytesData, _ := json.Marshal(data)
		go func() {
			_, err := http.Post(url, "application/json", bytes.NewBuffer(bytesData))
			if err != nil {
				fmt.Println("Sync commit fail")
			}
		}()
	}
}

// 创建新表
func CreateHandler(c *gin.Context) {
	//检查是否为master状态
	if checkStatus(c) {
		return
	}

	var stmt SqlStatement

	if err := c.BindJSON(&stmt); err != nil {
		// 绑定statement失败，返回错误信息
		c.JSON(http.StatusBadRequest, dto.ResponseType[string]{
			Success: false,
			Data:    "null",
			ErrCode: "400",
			ErrMsg:  "parameter error",
		})
		return
	}

	// 判断该 req 是否已经执行过
	if checkReq(c, stmt.ReqId) {
		return
	}

	// 开启事务
	var txn, err = database.Mysql.Begin()

	// 执行语句
	fmt.Print(stmt.Statement)
	_, err = txn.Exec(stmt.Statement)
	// sql执行有误
	var driverError *mysql.MySQLError
	if errors.As(err, &driverError) {
		fmt.Printf("Execution error! err: %v\n", driverError)
		if driverError.Number == 1050 { // 表不存在
			c.JSON(http.StatusBadRequest, dto.ResponseType[string]{
				Success: false,
				ErrCode: "1050",
				ErrMsg:  "Table exists",
				Data:    "null",
			})
			return
		}
	}
	// 执行成功，获取 slave IP，准备同步
	fmt.Println("Execution success!")
	slaves := server.Rs.GetNodes()
	// 表同步
	syncRes := tableSync(slaves, stmt)
	if syncRes {
		// slave 均无误后，自己提交txn
		_ = txn.Commit()
		reqQueue.Add(stmt.ReqId) // 添加 reqId
		c.JSON(http.StatusOK, dto.ResponseType[string]{
			Success: true,
			ErrCode: "200",
			ErrMsg:  "Write successfully",
			Data:    "null",
		})

		var regionId = server.Rs.RegionId
		server.Rs.PutKey("/table/"+stmt.TableName, string(regionId))

	} else {
		//同步有误，回滚操作
		_ = txn.Rollback()
		c.JSON(http.StatusInternalServerError, dto.ResponseType[string]{
			Success: false,
			ErrCode: "500",
			ErrMsg:  "Sync error",
			Data:    "null",
		})
	}
	// 向 slave 同步 commit 信号
	for ip := range slaves {
		url := fmt.Sprintf("http://%s:%s/api/table/commit", ip, "8080")
		data := make(map[string]interface{})
		data["reqId"] = stmt.ReqId
		data["isCommit"] = syncRes
		bytesData, _ := json.Marshal(data)
		go func() {
			_, err := http.Post(url, "application/json", bytes.NewBuffer(bytesData))
			if err != nil {
				fmt.Println("Sync commit fail")
			}
		}()
	}
}

// 删除已有的表
func DeleteHandler(c *gin.Context) {
	//检查是否为master状态
	if checkStatus(c) {
		return
	}
	var stmt SqlStatement
	if err := c.BindJSON(&stmt); err != nil {
		// 绑定statement失败，返回错误信息
		c.JSON(http.StatusBadRequest, dto.ResponseType[string]{
			Success: false,
			Data:    "",
			ErrCode: "400",
			ErrMsg:  "parameter error",
		})
		return
	}

	// 判断该 req 是否已经执行过
	if checkReq(c, stmt.ReqId) {
		return
	}

	// 开启事务
	var txn, err = database.Mysql.Begin()

	// 执行语句
	_, err = txn.Exec(stmt.Statement)
	// sql执行有误
	var driverError *mysql.MySQLError
	if errors.As(err, &driverError) {
		fmt.Printf("Execution error! err: %v\n", driverError)
		if driverError.Number == 1146 {
			// 表不存在
			c.JSON(http.StatusBadRequest, dto.ResponseType[string]{
				Success: false,
				ErrCode: "1146",
				ErrMsg:  "Table not exist",
				Data:    "null",
			})
			return
		}
	}

	// 执行成功，获取 slave IP，准备同步
	fmt.Println("Execution success!")
	slaves := server.Rs.GetNodes()
	// 表同步
	syncRes := tableSync(slaves, stmt)
	if syncRes {
		// slave 均无误后，自己提交txn
		_ = txn.Commit()
		reqQueue.Add(stmt.ReqId) // 添加 reqId
		c.JSON(http.StatusOK, dto.ResponseType[string]{
			Success: true,
			ErrCode: "200",
			ErrMsg:  "Write successfully",
			Data:    "null",
		})
	} else {
		//同步有误，回滚操作
		_ = txn.Rollback()
		c.JSON(http.StatusInternalServerError, dto.ResponseType[string]{
			Success: false,
			ErrCode: "500",
			ErrMsg:  "Sync error",
			Data:    "null",
		})
	}
	// 向 slave 同步 commit 信号
	for ip := range slaves {
		url := fmt.Sprintf("http://%s:%s/api/table/commit", ip, "8080")
		data := make(map[string]interface{})
		data["reqId"] = stmt.ReqId
		data["isCommit"] = syncRes
		bytesData, _ := json.Marshal(data)
		go func() {
			_, err := http.Post(url, "application/json", bytes.NewBuffer(bytesData))
			if err != nil {
				fmt.Println("Sync commit fail")
			}
		}()
	}
}

// 判断 server 状态是否正确
func checkStatus(c *gin.Context) bool {
	if !server.Rs.IsMaster {
		c.JSON(http.StatusForbidden, dto.ResponseType[string]{
			Success: false,
			ErrCode: "403", //令客户端缓存失效的错误码
			ErrMsg:  "Bad server!",
			Data:    "null",
		})
		return true
	}
	return false
}

// 判断请求是否已经执行过，若执行过则跳过并返回成功
func checkReq(c *gin.Context, reqId string) bool {
	if reqQueue.IsExisted(reqId) {
		c.JSON(http.StatusOK, dto.ResponseType[string]{
			Success: true,
			ErrCode: "200",
			ErrMsg:  "Executed successfully!",
			Data:    "null",
		})
		return true
	}
	return false
}

func updateVisits(tableName string, cnt int) {
	// 初始化新表的访问量
	if _, ok := server.Rs.Visits[tableName]; !ok {
		server.Rs.Visits[tableName] = 0
	}
	server.Rs.Visits[tableName] += cnt
}

// 执行表同步，返回同步结果（是否成功）
func tableSync(ips []string, stmt SqlStatement) bool {
	// 创建一个等待所有请求完成的等待组
	var wg sync.WaitGroup
	// 放置请求结果的通道
	results := make(chan *http.Response, len(ips))
	// 构造 post 请求参数
	data := make(map[string]string)
	data["reqId"] = stmt.ReqId
	data["statement"] = stmt.Statement
	bytesData, _ := json.Marshal(data)
	for _, ip := range ips {
		wg.Add(1)
		url := fmt.Sprintf("http://%s:%s/api/table/sync", ip, "8080")
		//开启 new goroutine 发送1个请求
		go func() {
			defer wg.Done() //执行完成时等待-1
			resp, _ := http.Post(url, "application/json", bytes.NewBuffer(bytesData))
			defer resp.Body.Close()
			results <- resp //将响应内容发送到通道中
		}()
	}
	go func() {
		wg.Wait()
		close(results) //完成所有请求后关闭通道
	}()
	// 接受请求结果直到通道关闭
	syncSuccess := true
	for res := range results {
		if res == nil || res.StatusCode != http.StatusOK {
			//请求失败
			fmt.Println("Sync request error")
			syncSuccess = false
		}
	}
	return syncSuccess
}

func MoveHandler(c *gin.Context) {
	var params MoveParams
	if err := c.BindJSON(&params); err != nil {
		response := dto.ResponseType[string]{
			Success: false,
			Data:    "",
			ErrCode: "400",
			ErrMsg:  "Failed to parse the params",
		}
		c.JSON(400, response)
		return
	}
	cmd := exec.Command("mysqldump", viper.GetString("database.dbname"),
		params.TableName, "-u"+viper.GetString("database.username"),
		"-p"+viper.GetString("database.password"), "--skip-comments")
	stdout, err := cmd.Output()

	if err != nil {
		response := dto.ResponseType[string]{
			Success: false,
			Data:    "",
			ErrCode: "500",
			ErrMsg:  "Failed to dump the table",
		}
		c.JSON(500, response)
		return
	}

	// 开始记录新的 sql 语句
	messageQueue.Activate = true

	receiveParams := ReceiveParams{
		Statements: string(stdout),
	}
	postBody, err := json.Marshal(receiveParams)
	resp, err := http.Post("http://"+params.Destination+":8080/api/table/receive",
		"application/json", bytes.NewBuffer(postBody))

	if err != nil || resp.StatusCode != 200 {
		response := dto.ResponseType[string]{
			Success: false,
			Data:    "",
			ErrCode: "500",
			ErrMsg:  "Failed to send data to destination",
		}

		c.JSON(500, response)
		messageQueue.Activate = false
		return
	}

	for len(messageQueue.Statements) > 0 {
		messageQueue.Mutex.Lock()
		chaseParams := ChaseParams{
			Statements: messageQueue.Statements,
		}
		messageQueue.Statements = []string{}
		messageQueue.Mutex.Unlock()
		postBody, err := json.Marshal(chaseParams)
		resp, err := http.Post("http://"+params.Destination+":8080/api/table/chase",
			"application/json", bytes.NewBuffer(postBody))

		if err != nil || resp.StatusCode != 200 {
			messageQueue.Activate = false
			response := dto.ResponseType[string]{
				Success: false,
				Data:    "",
				ErrCode: "500",
				ErrMsg:  "Failed to chase the updates",
			}
			c.JSON(500, response)
			return
		}

		println("delay")
		time.Sleep(5 * time.Second)
	}

	messageQueue.Activate = false

	server.Rs.DeleteKey("/table/" + params.TableName)
	regionId := server.Rs.GetKey("/server/" + params.Destination)
	server.Rs.PutKey("/table/"+params.TableName, regionId)

	_, err = database.Mysql.Exec("DROP TABLE IF EXISTS " + params.TableName)
	chaseParams := ChaseParams{
		Statements: []string{"DROP TABLE IF EXISTS " + params.TableName},
	}
	postBody, _ = json.Marshal(chaseParams)
	slaves := server.Rs.GetSlaves()
	for _, slave := range slaves {
		resp, err := http.Post("http://"+slave+":8080/api/table/slave/chase",
			"application/json", bytes.NewBuffer(postBody))
		if err != nil || resp.StatusCode != 200 {
		}
	}

	response := dto.ResponseType[string]{
		Success: true,
		Data:    "Done",
		ErrCode: "200",
		ErrMsg:  "",
	}
	c.JSON(200, response)
	return
}

func ReceiveHandler(c *gin.Context) {
	var params ReceiveParams
	if err := c.BindJSON(&params); err != nil {
		response := dto.ResponseType[string]{
			Success: false,
			Data:    "",
			ErrCode: "400",
			ErrMsg:  "Failed to parse the params",
		}
		c.JSON(400, response)
		return
	}

	cmd := exec.Command("mysql", "-u"+viper.GetString("database.username"),
		"-p"+viper.GetString("database.password"), viper.GetString("database.dbname"))
	cmd.Stdin = bytes.NewBufferString(params.Statements)
	err := cmd.Run()
	if err != nil {
		response := dto.ResponseType[string]{
			Success: false,
			Data:    "",
			ErrCode: "500",
			ErrMsg:  "Failed to import the dumped data",
		}
		c.JSON(500, response)
		return
	}

	slaves := server.Rs.GetSlaves()
	for _, slave := range slaves {
		postBody, err := json.Marshal(params)
		resp, err := http.Post("http://"+slave+":8080/api/table/slave/receive",
			"application/json", bytes.NewBuffer(postBody))
		if err != nil || resp.StatusCode != 200 {
			response := dto.ResponseType[string]{
				Success: false,
				Data:    "",
				ErrCode: "500",
				ErrMsg:  "Failed to send data to slave",
			}
			c.JSON(500, response)
		}
	}

	response := dto.ResponseType[string]{
		Success: true,
		Data:    "Done",
		ErrCode: "200",
		ErrMsg:  "",
	}
	c.JSON(200, response)
	return
}

func ChaseHandler(c *gin.Context) {
	var params ChaseParams
	if err := c.BindJSON(&params); err != nil {
		response := dto.ResponseType[string]{
			Success: false,
			Data:    "",
			ErrCode: "400",
			ErrMsg:  "Failed to parse the params",
		}
		c.JSON(400, response)
		return
	}

	begin, _ := database.Mysql.Begin()

	for _, statement := range params.Statements {
		_, err := begin.Exec(statement)
		if err != nil {
		}
	}

	slaves := server.Rs.GetSlaves()
	for _, slave := range slaves {
		postBody, err := json.Marshal(params)
		resp, err := http.Post("http://"+slave+":8080/api/table/slave/chase",
			"application/json", bytes.NewBuffer(postBody))
		if err != nil || resp.StatusCode != 200 {
			response := dto.ResponseType[string]{
				Success: false,
				Data:    "",
				ErrCode: "500",
				ErrMsg:  "Failed to send data to slave",
			}
			_ = begin.Rollback()
			c.JSON(500, response)
		}
	}

	_ = begin.Commit()

	response := dto.ResponseType[string]{
		Success: true,
		Data:    "Done",
		ErrCode: "200",
		ErrMsg:  "",
	}
	c.JSON(200, response)
	return
}

type MoveParams struct {
	TableName   string `json:"tableName"`
	Destination string `json:"destination"`
}

type ReceiveParams struct {
	Statements string `json:"statements"`
}

type ChaseParams struct {
	Statements []string `json:"statements"`
}

type MessageQueue struct {
	Statements []string
	Activate   bool
	Mutex      *sync.RWMutex
}

type QueryResponse struct {
	Cols []string   `json:"cols"`
	Rows [][]string `json:"rows"`
}
