// Package dto: 用来定义 api 响应格式
package dto

// ResponseType 定义 API 的统一响应格式
type ResponseType[T any] struct {
	Success bool   `json:"success"`
	Data    T      `json:"data"`
	ErrCode string `json:"errCode"`
	ErrMsg  string `json:"errMsg"`
}

type QueryTableResponse struct {
	Name string `json:"name"`
	IP   string `json:"ip"`
}

type IPResponse struct {
	IP string `json:"ip"`
}

type ShowTableResponse struct {
	TableNames []string `json:"tableNames"`
}
