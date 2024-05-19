package main

import (
	"Region/database"
	"Region/route"
	"Region/server"
	"os"
	"os/signal"
	"strconv"
)

func main() {
	//读取配置文件
	server.Rs.ReadConfig()
	// 连接到etcd
	server.Rs.ConnectToEtcd()
	// 连接数据库,并初始化全局Mysql对象
	database.Mysql = database.InitDB()
	defer server.Rs.ExitFromEtcd()
	// 初始化路由
	r := route.SetupRouter()
	// 监听在配置中的指定端口
	go func() {
		err := r.Run("0.0.0.0:" + strconv.Itoa(server.Port))
		if err != nil {
			return
		}
	}()

	// Wait for interrupt signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	<-quit
}
