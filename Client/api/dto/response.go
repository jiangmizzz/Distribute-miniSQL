package dto

// ResponseType 定义 API 的统一响应格式
type ResponseType[T any] struct {
	Success bool   `json:"success"`
	Data    T      `json:"data"`
	ErrCode string `json:"errCode"`
	ErrMsg  string `json:"errMsg"`
}

//获取多张表对应的 ip
type QueryTableRequest struct {
	TableNames []string `json:"tableNames"`
}
type QueryTableResponse struct {
	Name string `json:"name"`
	IP   string `json:"ip"`
}

//建表
type NewTableRequest struct {
	TableName string `json:"tableName"`
}
//删表
type DeleteTableRequest struct {
	TableName string `json:"tableName"`
}
type IPResponse struct {
	IP string `json:"ip"`
}

//查询所有表
type ShowTableResponse struct {
	TableNames []string `json:"tableNames"`
}

//读SQL语句
type ReadSQLRequest struct {
	ReqId      string `json:"reqId"`
	TableName  string `json:"tableName"`
	Statement  string `json:"statement"`
}
type ReadSQLResponse struct {
	Cols []string   `json:"cols"`
	Rows [][]string `json:"rows"`
}

//写SQL语句
type WriteSQLRequest struct {
	ReqId     string `json:"reqId"`
	TableName string `json:"tableName"`
	Statement string `json:"statement"`
}

//建表
type CreateSQLRequest struct {
	ReqId     string `json:"reqId"`
	TableName string `json:"tableName"`
	Statement string `json:"statement"`
}
//删表
type DropSQLRequest struct {
	ReqId     string `json:"reqId"`
	TableName string `json:"tableName"`
	Statement string `json:"statement"`
}