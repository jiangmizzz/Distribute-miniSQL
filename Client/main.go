package main

import (
	"Client/api/dto" // 用了go.mod就好了
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"bufio"
	"strings"
	"time"
	"regexp"
)

var masterIP string
var tableIPs = make(map[string]string)
var ip string

//支持的SQL语句类型
const (
	SHOW_TABLES = 0
	CREATE_TABLE = 1
	DROP_TABLE = 2
	ALTER_TABLE = 3
	CREATE_INDEX = 4
	DELETE_LINE = 5
	INSERT_LINE = 6
	SELECT_SINGLE = 7
	SELECT_MULTI = 8
	EXIT = 9
	OTHER = 10
)

func main() { //下面开始写分布式数据库的客户端的代码（使用命令行交互）

	fmt.Println("Welcome to the MySQL monitor.  Commands end with ;")

	ip, _ = getLocalIP()//获取本地ip

	fmt.Println("Please input the IP address of the server you want to connect to.")
	fmt.Print("IP address: ")
	fmt.Scanln(&masterIP)
	
	//向这个ip和端口发送一个连接请求，如果连接成功，则打印连接成功，否则打印连接失败
	//测试master服务器是否正常工作
	conn, err := net.Dial("tcp", masterIP + ":8080") 
	if err != nil {
        fmt.Println("connect failed! err:", err)
        return
    }
    defer conn.Close()
    fmt.Println("connect success!")

	fmt.Println("Welcome to the MySQL monitor.  Commands end with ;")

	for{//下面接收用户输入的SQL语句

		reader := bufio.NewReader(os.Stdin)
		var inputLines string
		isFirstInput := true
		for {
			if isFirstInput {
				fmt.Print("mysql> ")
				isFirstInput = false
			} else {
				fmt.Print("    -> ")
			}
			input, _ := reader.ReadString('\n')
			input = strings.TrimSuffix(input, "\r\n") //Windows系统下的换行符是\r\n

			if inputLines != "" {
				inputLines += " "
			}
			inputLines += input

			if input == ";" { //当前行的输入单走一个分号
				break
			}
			if strings.Contains(input, ";") { //当前行的输入包含分号，只保留分号前的部分
				inputLines = strings.Split(inputLines, ";")[0]
				inputLines += ";"
				break
			}
		}

		if(inputLines == ";"){
			fmt.Println("ERROR:\r\nNo query specified")
			continue
		}

		tables, sqlType := parseSQL(inputLines)

		for i, table := range tables { //如果tables中有某表以"("结尾，去掉结尾的"("
			if strings.HasSuffix(table, "(") {
				tables[i] = strings.TrimSuffix(table, "(")
			}
		}

		fmt.Println(tables, sqlType, inputLines)

		switch sqlType {
			case SHOW_TABLES: //show tables
				names := showTables()
				for _, name := range names {
					fmt.Println(name)
				}

			case CREATE_TABLE: //create table tableName (...)
				if len(tables) == 0 {
					fmt.Println("Invalid request")
					break
				}
				ip := newTable(tables[0])
				if ip == "" {
					fmt.Println("Failed to create table")
				} else {
					tableIPs[tables[0]] = ip
					flag := regionCreateTable(tables[0], inputLines)
					if flag {
						fmt.Println("Table created")
					} else {
						fmt.Println("Failed to create table")
					}
				}

			case DROP_TABLE: //drop table tableName
				if len(tables) == 0 {
					fmt.Println("Invalid request")
					break
				}
				ip := deleteTable(tables[0])
				if ip == "" {
					fmt.Println("Failed to delete table")
				} else {
					tableIPs[tables[0]] = ip
					flag := regionDeleteTable(tables[0], inputLines)
					if flag {
						fmt.Println("Table deleted")
					} else {
						fmt.Println("Failed to delete table")
					}
				}

			case ALTER_TABLE: //alter table tableName ...
				flag := writeSQL(tables[0], inputLines)
				if flag {
					fmt.Println("Table altered")
				} else {
					fmt.Println("Failed to alter table")
				}

			case CREATE_INDEX: //create index indexName on tableName (colName)
				flag := writeSQL(tables[0], inputLines)
				if flag {
					fmt.Println("Index created")
				} else {
					fmt.Println("Failed to create index")
				}

			case DELETE_LINE: //delete from tableName where ...
				flag := writeSQL(tables[0], inputLines)
				if flag {
					fmt.Println("Line deleted")
				} else {
					fmt.Println("Failed to delete line")
				}

			case INSERT_LINE: //insert into tableName values (...)
				flag := writeSQL(tables[0], inputLines)
				if flag {
					fmt.Println("Line inserted")
				} else {
					fmt.Println("Failed to insert line")
				}

			case SELECT_SINGLE: //select *(c1,c2,...) from tableName where ...
				cols, rows := readSQL(tables[0], inputLines)
				if len(cols) == 0 {
					fmt.Println("Failed to read table")
				} else {
					for _, col := range cols {
						fmt.Printf("%s\t", col)
					}
					fmt.Println()
					for _, row := range rows {
						for _, cell := range row {
							fmt.Printf("%s\t", cell)
						}
						fmt.Println()
					}
				}

			case SELECT_MULTI: //select *(c1,c2,...) from tableName1,tableName2 where ...
				// 解析输入的SQL语句
				re := regexp.MustCompile(`(?i)select\s+(.*?)\s+from\s+(.*)`)//正则匹配
				matches := re.FindStringSubmatch(inputLines)
				if len(matches) < 3 {
					fmt.Println("Invalid request")
					break
				}
			
				selectFields := strings.Split(matches[1], ",")//属性名数组（也可能只有一个*）
				tables := strings.Split(matches[2], ",")//表名数组
			
				var allCols []string
				var allRows [][]string
			
				// 对每个表进行处理
				for i, table := range tables {
					table = strings.TrimSpace(table)
					fields := []string{}
			
					// 找出该表的字段
					for _, field := range selectFields {
						if strings.HasPrefix(strings.TrimSpace(field), table+".") {
							fields = append(fields, strings.TrimSpace(field))
						}
					}
			
					// 调用readSQL函数处理简短的SQL语句
					cols, rows := readSQL(table,fmt.Sprintf("SELECT %s FROM %s", strings.Join(fields, ","), table))
			
					// 如果是第一个表，直接将cols和rows赋值给allCols和allRows
					if i == 0 {
						allCols = cols
						allRows = rows
					} else {
						// 如果不是第一个表，做笛卡尔积
						var newRows [][]string
						for _, row1 := range allRows {
							for _, row2 := range rows {
								newRow := append(row1, row2...)
								newRows = append(newRows, newRow)
							}
						}
						allRows = newRows
						allCols = append(allCols, cols...)
					}
				}
				for _, col := range allCols {
					fmt.Printf("%s\t", col)
				}
				fmt.Println()
				for _, row := range allRows {
					for _, cell := range row {
						fmt.Printf("%s\t", cell)
					}
					fmt.Println()
				}

			case EXIT: //exit
				fmt.Println("Bye")
				return

			case OTHER: //其他SQL语句
				fmt.Println("Invalid request")
		}	
	}
}

//下面是工具函数
//获取IP
func getLocalIP() (string, error) {
    conn, err := net.Dial("udp", "8.8.8.8:80")
    if err != nil {
        return "", err
    }
    defer conn.Close()

    localAddr := conn.LocalAddr().(*net.UDPAddr)
    return localAddr.IP.String(), nil
}

//查询所有表（show tables）
func showTables() []string{
	resp, err := http.Get("http://" + masterIP + ":8080/api/table/show")
	if err != nil {
		fmt.Println("Request failed:", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	var response dto.ResponseType[dto.ShowTableResponse]
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		fmt.Println("Decoding failed:", err)
		os.Exit(1)
	}
	if !response.Success {
		fmt.Println("Request failed:", response.ErrMsg)
		os.Exit(1)
	}

	tablesName := response.Data.TableNames

	if len(tablesName) == 0 {
		fmt.Println("Empty set")
		return tablesName
	}

	//获取这些表名对应的ip
	getTableIPs(tablesName)

	fmt.Println("--------------")
	fmt.Println("Tables_in_test")
	fmt.Println("--------------")
	return tablesName
}

//建表（master），GET方法请求参数在url中
//传入表名，返回表的ip（详见NewTableRequest和IPResponse），路径是table/query，GET方法参数
func newTable(tableName string) string {
	resp, err := http.Get("http://" + masterIP + ":8080/api/table/new?tableName=" + tableName)
	if err != nil {
		fmt.Println("Request failed:", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	//针对resp.StatusCode的不同，而做不同的处理（200/400/409/500/503）
	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == http.StatusBadRequest {
			fmt.Println("Request failed:", "Invalid request")
		} else if resp.StatusCode == http.StatusConflict {
			fmt.Println("Request failed:", "Table already exist")
		} else if resp.StatusCode == http.StatusInternalServerError {
			fmt.Println("Request failed:", "Failed to create table")
		} else if resp.StatusCode == http.StatusServiceUnavailable {
			fmt.Println("Request failed:", "No enough region servers")
		}
	}

	var response dto.ResponseType[dto.IPResponse]
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		fmt.Println("Decoding failed:", err)
		os.Exit(1)
	}
	if !response.Success {
		fmt.Println("Request failed:", response.ErrMsg)
		os.Exit(1)
	}

	return response.Data.IP
}

//删表（master），GET方法请求参数在url中
func deleteTable (tableName string) string {
	resp, err := http.Get("http://" + masterIP + ":8080/api/table/delete?tableName=" + tableName)
	if err != nil {
		fmt.Println("Request failed:", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	//针对resp.StatusCode的不同，而做不同的处理（200/400/409/500/503）
	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == http.StatusBadRequest {
			fmt.Println("Request failed:", "Invalid request")
		} else if resp.StatusCode == http.StatusConflict {
			fmt.Println("Request failed:", "Table already exist")
		} else if resp.StatusCode == http.StatusInternalServerError {
			fmt.Println("Request failed:", "Failed to create table")
		} else if resp.StatusCode == http.StatusServiceUnavailable {
			fmt.Println("Request failed:", "No enough region servers")
		}
	}

	var response dto.ResponseType[dto.IPResponse]
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		fmt.Println("Decoding failed:", err)
		os.Exit(1)
	}
	if !response.Success {
		fmt.Println("Request failed:", response.ErrMsg)
		os.Exit(1)
	}

	return response.Data.IP
}

// 获取多张表对应的 ip，并存入tableIPs中
func getTableIPs(tableNames []string) {
	jsonBody, err := json.Marshal(dto.QueryTableRequest{TableNames: tableNames})
	if err != nil {
		fmt.Println("Decoding failed:", err)
		os.Exit(1)
	}

	resp, err := http.Post("http://" + masterIP + ":8080/api/table/query", "application/json", bytes.NewReader(jsonBody))
	if err != nil {
		fmt.Println("Request failed:", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	//针对resp.StatusCode的不同，而做不同的处理
	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == http.StatusBadRequest {
			fmt.Println("Request failed:", "Invalid request")
		} else if resp.StatusCode == http.StatusInternalServerError {
			fmt.Println("Request failed:", "Failed to query table")
		}
	}

	var response dto.ResponseType[[]dto.QueryTableResponse]
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		fmt.Println("Decoding failed:", err)
		os.Exit(1)
	}
	if !response.Success {
		fmt.Println("Request failed:", response.ErrMsg)
		os.Exit(1)
	}

	//将表名和ip对应关系存入tableIPs
	for _, table := range response.Data {
		tableIPs[table.Name] = table.IP //查不到的ip为空，不会出现问题
	}
}

//读SQL语句,向指定ip的表发送读请求，返回cols和rows（单表可直接打印，多表需要笛卡尔积）
func readSQL(tableName string, sql string) (cols []string, rows [][]string) {
	unixNanoTimeString := fmt.Sprintf("%d", time.Now().UnixNano())//获取当前时间戳
	jsonBody, err := json.Marshal(dto.ReadSQLRequest{ReqId: ip+unixNanoTimeString, TableName: tableName, Statement: sql})
	if err != nil {
		fmt.Println("Decoding failed:", err)
		os.Exit(1)
	}

	//检查缓存tableIPs中是否有这个表的ip，如果有，直接发送请求，如果没有，先调用getTableIPs获取
	if tableIPs[tableName] == "" {
		getTableIPs([]string{tableName})
	}

	resp, err := http.Post("http://" + tableIPs[tableName] + ":8080/api/sql/read", "application/json", bytes.NewReader(jsonBody))
	if err != nil {
		fmt.Println("Request failed:", err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	
	var response dto.ResponseType[dto.ReadSQLResponse]

	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		fmt.Println("Decoding failed:", err)
		os.Exit(1)
	}
	if !response.Success {
		if response.ErrCode == "403" {
			fmt.Println("Request failed:", "Refresh cache")
			tableIPs[tableName] = ""//清空这个表的ip，下次再请求时会重新获取
		} else if response.ErrCode == "1146" {
			fmt.Println("Request failed:", "Table doesn't exist")
			tableIPs[tableName] = ""//清空这个表的ip，下次再请求时会重新获取
		} else if response.ErrCode == "400" {  
			fmt.Println("Request failed:", "Invalid request")
		} else {
			fmt.Println("Request failed:", response.ErrMsg)
		}
		os.Exit(1)
	}

	return response.Data.Cols, response.Data.Rows
}

//写SQL语句,向指定ip的表发送写请求
func writeSQL(tableName string, sql string) bool {
	unixNanoTimeString := fmt.Sprintf("%d", time.Now().UnixNano())//获取当前时间戳
	jsonBody, err := json.Marshal(dto.WriteSQLRequest{ReqId: ip+unixNanoTimeString, TableName: tableName, Statement: sql})
	if err != nil {
		fmt.Println("Decoding failed:", err)
		os.Exit(1)
	}

	//检查缓存tableIPs中是否有这个表的ip，如果有，直接发送请求，如果没有，先调用getTableIPs获取
	if tableIPs[tableName] == "" {
		getTableIPs([]string{tableName})
	}

	resp, err := http.Post("http://" + tableIPs[tableName] + ":8080/api/sql/write", "application/json", bytes.NewReader(jsonBody))
	if err != nil {
		fmt.Println("Request failed:", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	//创建dto.ResponseType，里面的泛型为空（这个函数不需要返回值）
	var response dto.ResponseType[interface{}]
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		fmt.Println("Decoding failed:", err)
		os.Exit(1)
	}
	if !response.Success {
		if response.ErrCode == "403" {
			fmt.Println("Request failed:", "Refresh cache")
			tableIPs[tableName] = ""//清空这个表的ip，下次再请求时会重新获取
		} else if response.ErrCode == "1146" {
			fmt.Println("Request failed:", "Table doesn't exist")
			tableIPs[tableName] = ""//清空这个表的ip，下次再请求时会重新获取
		} else if response.ErrCode == "400" {  
			fmt.Println("Request failed:", "Invalid request")
		} else {
			fmt.Println("Request failed:", response.ErrMsg)
		}
		return false
	}
	return true
}

//建表（region），POST方法
func regionCreateTable(tableName string, sql string) bool {
	unixNanoTimeString := fmt.Sprintf("%d", time.Now().UnixNano())//获取当前时间戳
	jsonBody, err := json.Marshal(dto.CreateSQLRequest{ReqId: ip+unixNanoTimeString, TableName: tableName, Statement: sql})
	if err != nil {
		fmt.Println("Decoding failed:", err)
		os.Exit(1)
	}

	//检查缓存tableIPs中是否有这个表的ip，如果有，直接发送请求，如果没有，先调用getTableIPs获取
	if tableIPs[tableName] == "" {
		getTableIPs([]string{tableName})
	}

	resp, err := http.Post("http://" + tableIPs[tableName] + ":8080/api/sql/create", "application/json", bytes.NewReader(jsonBody))
	if err != nil {
		fmt.Println("Request failed:", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	//创建dto.ResponseType，里面的泛型为空（这个函数不需要返回值）
	var response dto.ResponseType[interface{}]
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		fmt.Println("Decoding failed:", err)
		os.Exit(1)
	}
	if !response.Success {
		if response.ErrCode == "403" {
			fmt.Println("Request failed:", "Refresh cache")
			tableIPs[tableName] = ""//清空这个表的ip，下次再请求时会重新获取
		} else if response.ErrCode == "1146" {
			fmt.Println("Request failed:", "Table doesn't exist")
			tableIPs[tableName] = ""//清空这个表的ip，下次再请求时会重新获取
		} else if response.ErrCode == "400" {  
			fmt.Println("Request failed:", "Invalid request")
		} else {
			fmt.Println("Request failed:", response.ErrMsg)
		}
		return false
	}
	return true
}

//删表（region），POST方法
func regionDeleteTable(tableName string, sql string) bool {
	unixNanoTimeString := fmt.Sprintf("%d", time.Now().UnixNano())//获取当前时间戳
	jsonBody, err := json.Marshal(dto.CreateSQLRequest{ReqId: ip+unixNanoTimeString, TableName: tableName, Statement: sql})
	if err != nil {
		fmt.Println("Decoding failed:", err)
		os.Exit(1)
	}

	//检查缓存tableIPs中是否有这个表的ip，如果有，直接发送请求，如果没有，先调用getTableIPs获取
	if tableIPs[tableName] == "" {
		getTableIPs([]string{tableName})
	}
	
	resp, err := http.Post("http://" + tableIPs[tableName] + ":8080/api/sql/delete", "application/json", bytes.NewReader(jsonBody))
	if err != nil {
		fmt.Println("Request failed:", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	//创建dto.ResponseType，里面的泛型为空（这个函数不需要返回值）
	var response dto.ResponseType[interface{}]
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		fmt.Println("Decoding failed:", err)
		os.Exit(1)
	}
	if !response.Success {
		if response.ErrCode == "403" {
			fmt.Println("Request failed:", "Refresh cache")
			tableIPs[tableName] = ""//清空这个表的ip，下次再请求时会重新获取
		} else if response.ErrCode == "1146" {
			fmt.Println("Request failed:", "Table doesn't exist")
			tableIPs[tableName] = ""//清空这个表的ip，下次再请求时会重新获取
		} else if response.ErrCode == "400" {  
			fmt.Println("Request failed:", "Invalid request")
		} else {
			fmt.Println("Request failed:", response.ErrMsg)
		}
		return false
	}
	return true
}

func parseSQL(sql string) ([]string, int) {
    sql = strings.TrimSpace(sql)
    words := strings.Fields(sql)
	if words[len(words)-1] == ";" {//去掉分号
		words = words[:len(words)-1]
	}
	words[len(words)-1] = strings.TrimSuffix(words[len(words)-1], ";")

	for i, word := range words { //MySQL不区分大小写，全部转成大写方便后续操作
		words[i] = strings.ToUpper(word)
	}

	for i, word := range words { //处理逗号分隔的表名
		if strings.Contains(word, ",") {
			if word[0] != ',' && word[len(word)-1] != ',' {
				words = append(words[:i], append([]string{strings.Split(word, ",")[0], strings.Split(word, ",")[1]}, words[i+1:]...)...)
			}
		}
	}

	for i, word := range words {//去掉逗号
		if word == "," {
			words = append(words[:i], words[i+1:]...)
		} else if strings.HasSuffix(word, ",") || strings.HasPrefix(word, ",") {
			word = strings.TrimSuffix(word, ",")
			word = strings.TrimPrefix(word, ",")
			words[i] = word
		}
	}

	// for i, word := range words {//输出words
	// 	fmt.Printf("%d: %s\n", i, word)
	// }

    var tables []string
    var sqlType int
	if words[0] == "SHOW" && words[1] == "TABLES" {
		sqlType = SHOW_TABLES
	} else if words[0] == "CREATE" && words[1] == "TABLE" {
		sqlType = CREATE_TABLE
		tables = append(tables, words[2])
	} else if words[0] == "DROP" && words[1] == "TABLE" {
		sqlType = DROP_TABLE
		tables = append(tables, words[2])
	} else if words[0] == "ALTER" && words[1] == "TABLE" {
		sqlType = ALTER_TABLE
		tables = append(tables, words[2])
	} else if words[0] == "CREATE" && words[1] == "INDEX" {
		sqlType = CREATE_INDEX
		tables = append(tables, words[4]) //输入的单词少于5个咋办？
	} else if words[0] == "DELETE" && words[1] == "FROM" {
		sqlType = DELETE_LINE
		tables = append(tables, words[2])
	} else if words[0] == "INSERT" && words[1] == "INTO" {
		sqlType = INSERT_LINE
		tables = append(tables, words[2])
	} else if words[0] == "SELECT" {
		sqlType = SELECT_SINGLE
		for i := 1; i < len(words); i++ {
			if words[i] == "FROM" {
				//从FROM后面的表名开始，直到遇到WHERE或者words结束
				for j := i+1; j < len(words); j++ {
					if words[j] == "WHERE" {
						break
					}
					tables = append(tables, words[j])
					if len(tables) > 1 {
						sqlType = SELECT_MULTI
					}
				}
			}
		}
	} else if words[0] == "EXIT" {
		sqlType = EXIT

	} else {
		sqlType = OTHER
	}

    return tables, sqlType
}