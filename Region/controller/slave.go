package controller

import (
	"Region/api/dto"
	"Region/database"
	"bytes"
	"github.com/gin-gonic/gin"
	"github.com/spf13/viper"
	"os/exec"
)

// 从master同步数据
func SyncHandler(c *gin.Context) {

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

	cmd := exec.Command("mysql", "-u"+viper.GetString("database.username"),
		"-p"+viper.GetString("database.password"), viper.GetString("database.dbname"))
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
