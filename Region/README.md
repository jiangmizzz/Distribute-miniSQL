# Region
## 编译运行

### 配置数据库连接
在 `./database` 目录下将 `dbconfig.yaml.template` 文件复制一份并命名为 `dbconfig.yaml`，并填写当前服务器上要连接的 MySQL 数据库的配置。

### 安装依赖
先配置 go 模块环境，设置 `GOPROXY` 来更换代理源，加速依赖安装：
```
GOPROXY=https://goproxy.cn
```
然后安装所有项目运行所需的依赖项: 
```shell
go mod tidy
```

### 运行项目
运行 `main.go` 主程序: 新添加go构建配置后运行，或者终端执行
```shell
go run main.go
```