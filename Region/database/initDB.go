package database

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/spf13/viper"
	"log/slog"
	"time"
)

var Mysql *sql.DB

func InitDB() *sql.DB {
	viper.SetConfigName("dbconfig")   // 配置文件名称(无扩展名)
	viper.SetConfigType("yaml")       // 如果配置文件的名称中没有扩展名，则需要配置此项
	viper.AddConfigPath("./database") // 查找配置文件所在的路径
	err := viper.ReadInConfig()       // 查找并读取配置文件
	if err != nil {
		slog.Error("Error reading config file, %s", err)
	}

	username := viper.GetString("database.username")
	password := viper.GetString("database.password")
	host := viper.GetString("database.host")
	port := viper.GetString("database.port")
	dbname := viper.GetString("database.dbname")

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8mb4&parseTime=True&loc=Local", username, password, host, port, dbname)
	if conn, err := sql.Open("mysql", dsn); err != nil {
		panic(err.Error())
	} else {
		conn.SetConnMaxLifetime(7 * time.Second) //设置空闲时间，这个是比mysql 主动断开的时候短
		conn.SetMaxOpenConns(10)
		conn.SetMaxIdleConns(10)
		return conn
	}
}
