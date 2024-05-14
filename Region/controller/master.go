package controller

import (
	"Region/api/dto"
	"Region/database"
	"Region/server"
	"bytes"
	"encoding/json"
	"github.com/gin-gonic/gin"
	"github.com/spf13/viper"
	"net/http"
	"os/exec"
	"sync"
	"time"
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
