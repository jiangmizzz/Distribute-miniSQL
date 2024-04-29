package Controller

import (
	"Master/api/dto"
	"github.com/gin-gonic/gin"
	"net/http"
	"strconv"
)

type Service interface {
	QueryTable(tableNames []string) ([]dto.Tables, error)
}

type Controller struct {
	Service Service
}

func (controller *Controller) QueryTable(c *gin.Context) {
	var body struct {
		TableNames []string `json:"tableNames"`
	}
	var response dto.ResponseType[[]dto.Tables]
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
