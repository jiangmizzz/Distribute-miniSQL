package main

import (
	"Region/database"
	"Region/route"
	"Region/server"
)

func main() {
	//连接到etcd
	server.Rs.ConnectToEtcd()

	//连接数据库,并初始化全局Mysql对象
	database.Mysql = database.InitDB()
	//初始化路由
	r := route.SetupRouter()
	//默认监听在 8080 端口
	err := r.Run("0.0.0.0:8080")
	if err != nil {
		return
	}
}
