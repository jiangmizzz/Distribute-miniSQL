package dto

type MoveTableRequest struct {
	TableName   string `json:"tableName"`
	Destination string `json:"destination"`
}

type SyncDataRequest struct {
	Destination string `json:"destination"`
}
