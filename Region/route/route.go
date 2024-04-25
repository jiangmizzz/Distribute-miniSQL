package route

import (
	"Region/controller"
	"github.com/gin-gonic/gin"
)

// 配置cors中间件（跨域）
func corsMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Writer.Header().Set("Access-Control-Allow-Origin", "*")
		c.Writer.Header().Set("Access-Control-Allow-Credentials", "true")
		c.Writer.Header().Set("Access-Control-Allow-Headers", "Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization, accept, origin, Cache-Control, X-Requested-With")
		c.Writer.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS, GET, PUT, DELETE")

		// 检查是否为预检请求
		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}

		c.Next()
	}
}

func SetupRouter() *gin.Engine {
	r := gin.Default()

	// 应用CORS中间件
	r.Use(corsMiddleware())

	apiRoutes := r.Group("/api")
	//配置路由
	apiRoutes.GET("/query/:sql", controller.QueryHandler)
	//其他路由...
	return r
}
