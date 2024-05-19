package controller

import (
	"Region/api/dto"
	"Region/database"
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"os/exec"
	"time"

	"github.com/gin-gonic/gin"
)

type SyncStatement struct {
	ReqId     string `json:"reqId"`
	Statement string `json:"statement"`
}

type CommitStatement struct {
	ReqId    string `json:"reqId"`
	IsCommit bool   `json:"isCommit"`
}

type Txn struct {
	txn *sql.Tx
	ctx context.Context
}

// 正在执行的事务列表
var TxnMap = make(map[string]Txn)

// 从master同步数据，开启事务
func SyncHandler(c *gin.Context) {
	var stmt SyncStatement
	c.BindJSON(&stmt)
	// 存在正在处理的事务
	if TxnMap[stmt.ReqId].txn != nil {
		c.JSON(http.StatusBadRequest, dto.ResponseType[string]{
			Success: false,
			Data:    "null",
			ErrCode: "400",
			ErrMsg:  "transaction exists",
		})
		return
	}
	var txn, err = database.Mysql.Begin()

	// 若当前事务超时，则撤销该操作
	ctx, _ := context.WithTimeout(context.Background(), 1500*time.Millisecond)

	// 事务超时后回滚
	go func() {
		<-ctx.Done()
		if TxnMap[stmt.ReqId].txn != nil {
			err := TxnMap[stmt.ReqId].txn.Rollback()
			if err != nil {
				fmt.Println("Rollback error:", err)
			}
			delete(TxnMap, stmt.ReqId)
		}
	}()

	TxnMap[stmt.ReqId] = Txn{txn, ctx}

	// 执行出错
	_, err = txn.Exec(stmt.Statement)
	if err != nil {
		c.JSON(http.StatusOK, dto.ResponseType[string]{
			Success: true,
			Data:    "null",
			ErrCode: "400",
			ErrMsg:  err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, dto.ResponseType[string]{
		Success: true,
		Data:    "null",
		ErrCode: "200",
		ErrMsg:  "success",
	})
	return

}

// 收到master的commit/rollback请求，执行/回滚事务
func CommitHandler(c *gin.Context) {
	var stmt CommitStatement
	if err := c.BindJSON(&stmt); err != nil {
		// 绑定statement失败，返回错误信息
		c.JSON(http.StatusBadRequest, dto.ResponseType[string]{
			Success: true,
			Data:    "",
			ErrCode: "400",
			ErrMsg:  "parameter error",
		})
		return
	}
	//fmt.Println(stmt.isCommit)
	if TxnMap[stmt.ReqId].txn == nil {
		c.JSON(http.StatusBadRequest, dto.ResponseType[string]{
			Success: true,
			Data:    "",
			ErrCode: "401",
			ErrMsg:  "transaction not exists",
		})
		return
	} else {
		if stmt.IsCommit {
			err := TxnMap[stmt.ReqId].txn.Commit()
			reqQueue.Add(stmt.ReqId)
			if err != nil {
				fmt.Println("Commit error:", err)
			}
		} else {
			err := TxnMap[stmt.ReqId].txn.Rollback()
			if err != nil {
				fmt.Println("Rollback error:", err)
			}
		}

		TxnMap[stmt.ReqId].ctx.Done()
		delete(TxnMap, stmt.ReqId)

		c.JSON(http.StatusOK, dto.ResponseType[string]{
			Success: true,
			Data:    "null",
			ErrCode: "200",
			ErrMsg:  "success",
		})
		return
	}

}

func SlaveReceiveHandler(c *gin.Context) {
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

	cmd := exec.Command("mysql", "-u"+database.Username,
		"-p"+database.Password, database.DBname)
	cmd.Stdin = bytes.NewBufferString(params.Statements)
	err := cmd.Run()
	if err != nil {
		response := dto.ResponseType[string]{
			Success: false,
			Data:    "",
			ErrCode: "500",
			ErrMsg:  "Failed to import dumped data",
		}
		c.JSON(500, response)
		return
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

func SlaveChaseHandler(c *gin.Context) {
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

	for _, statement := range params.Statements {
		_, err := database.Mysql.Exec(statement)
		if err != nil {
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
