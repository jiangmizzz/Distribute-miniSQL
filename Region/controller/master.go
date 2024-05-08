package controller

import (
	"Region/api/dto"
	"Region/database"
	"bytes"
	"encoding/json"
	"github.com/gin-gonic/gin"
	"github.com/spf13/viper"
	"net/http"
	"os/exec"
	"sync"
)

// 消息队列 当执行表迁移时开启，用于记录增量更新的 sql 语句
var messageQueue = MessageQueue{
	Statements: []string{},
	Activate:   false,
	Mutex:      &sync.RWMutex{},
}

// 用sql指令进行数据查询
func QueryHandler(c *gin.Context) {

}

// sql写操作
func WriteHandler(c *gin.Context) {

}

func MoveHandler(c *gin.Context) {
	var params MoveParams
	if err := c.BindJSON(&params); err != nil {
		response := dto.ResponseType[string]{
			Success: false,
			Data:    "",
			ErrCode: "400",
			ErrMsg:  err.Error(),
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
			ErrCode: "400",
			ErrMsg:  err.Error(),
		}
		c.JSON(400, response)
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
			ErrCode: "400",
			ErrMsg:  "Failed to send data to destination",
		}
		c.JSON(400, response)
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
		}
	}

	messageQueue.Activate = false

	// todo 修改 etcd 中的记录

	_, err = database.Mysql.Exec("DROP TABLE IF EXISTS " + params.TableName)

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
			ErrMsg:  err.Error(),
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
			ErrCode: "400",
			ErrMsg:  err.Error(),
		}
		c.JSON(400, response)
		return
	}

	// todo 将数据转发给 slave node

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
			ErrMsg:  err.Error(),
		}
		c.JSON(400, response)
		return
	}

	for _, statement := range params.Statements {
		_, err := database.Mysql.Exec(statement)
		if err != nil {
			// skip 错误的 sql 语句
		}
	}

	// todo 将数据转发给 slave node

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
