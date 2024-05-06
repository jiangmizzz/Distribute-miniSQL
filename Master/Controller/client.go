package Controller

import (
	"Master/api/dto"
	"github.com/gin-gonic/gin"
	"net/http"
	"strconv"
)

type Service interface {
	QueryTable(tableNames []string) ([]dto.QueryTableResponse, error)
	NewTable(tableName string) (string, error)
	DeleteTable(tableName string) (string, error)
}

type Controller struct {
	Service Service
}

func (controller *Controller) QueryTable(c *gin.Context) {
	var body struct {
		TableNames []string `json:"tableNames"`
	}
	var response dto.ResponseType[[]dto.QueryTableResponse]
	if err := c.BindJSON(&body); err != nil || len(body.TableNames) == 0 {
		response.Success = false
		response.ErrCode = strconv.Itoa(http.StatusBadRequest)
		response.ErrMsg = "Invalid request"
		c.JSON(http.StatusBadRequest, response)
		return
	}

	tables, err := controller.Service.QueryTable(body.TableNames)
	if err != nil {
		response.Success = false
		response.ErrCode = strconv.Itoa(http.StatusInternalServerError)
		response.ErrMsg = "Failed to query table"
		c.JSON(http.StatusInternalServerError, response)
		return
	}
	response.Success = true
	response.Data = tables
	c.JSON(http.StatusOK, response)
}

func (controller *Controller) NewTable(c *gin.Context) {
	tableName := c.Query("tableName")
	var response dto.ResponseType[dto.IPResponse]
	if tableName == "" {
		response.Success = false
		response.ErrCode = strconv.Itoa(http.StatusBadRequest)
		response.ErrMsg = "Invalid request"
		c.JSON(http.StatusBadRequest, response)
		return
	}

	ip, err := controller.Service.NewTable(tableName)
	if err != nil {
		response.Success = false
		if err.Error() == "table already exist" {
			response.ErrCode = strconv.Itoa(http.StatusConflict)
			response.ErrMsg = "Table already exist"
			c.JSON(http.StatusConflict, response)
		} else if err.Error() == "no enough region servers" {
			response.ErrCode = strconv.Itoa(http.StatusServiceUnavailable)
			response.ErrMsg = "No enough region servers"
			c.JSON(http.StatusServiceUnavailable, response)
		} else {
			response.ErrCode = strconv.Itoa(http.StatusInternalServerError)
			response.ErrMsg = "Internal server error"
			c.JSON(http.StatusInternalServerError, response)
		}
		return
	}
	response.Success = true
	response.Data = dto.IPResponse{IP: ip}
	c.JSON(http.StatusOK, response)
}

func (controller *Controller) DeleteTable(c *gin.Context) {
	tableName := c.Query("tableName")
	var response dto.ResponseType[dto.IPResponse]
	if tableName == "" {
		response.Success = false
		response.ErrCode = strconv.Itoa(http.StatusBadRequest)
		response.ErrMsg = "Invalid request"
		c.JSON(http.StatusBadRequest, response)
		return
	}

	ip, err := controller.Service.DeleteTable(tableName)
	if err != nil {
		response.Success = false
		if err.Error() == "table not exist" {
			response.ErrCode = strconv.Itoa(http.StatusNotFound)
			response.ErrMsg = "Table not exist"
			c.JSON(http.StatusNotFound, response)
		} else if err.Error() == "no enough region servers" {
			response.ErrCode = strconv.Itoa(http.StatusServiceUnavailable)
			response.ErrMsg = "No enough region servers"
			c.JSON(http.StatusServiceUnavailable, response)
		} else {
			response.ErrCode = strconv.Itoa(http.StatusInternalServerError)
			response.ErrMsg = "Internal server error"
			c.JSON(http.StatusInternalServerError, response)
		}
		return
	}
	response.Success = true
	response.Data = dto.IPResponse{IP: ip}
	c.JSON(http.StatusOK, response)
}
