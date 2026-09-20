// Package router 挂载 HTTP 路由与 App 组装。
//
// 与简历「Go 后端 — 五条链路」对应的代码入口（便于口述对齐实现）：
//
//  1 异步统计批量落库：阅读限频与 Redis 计数/榜单见 database/article_redis.go、service/article_service.go；
//    NSQ article_stats 与缓冲落库见 mq/article_consumer.go（StatsWorker / CommentStatsWorker）、mq/nsq_init.go。
//  2 积分入账：Redis 日上限 + MySQL 同事务钱包与流水见 service/points_service.go（EnqueueEarn / EarnPointsWithTxnID）；
//    Kafka topic points.earn、消费重试与死信 points.earn.dlq 见 mq/kafka_init.go，消费端 handler/points_handler.go；未启用 Kafka 时同入口降级同步。
//  3 积分商城：Redis Lua 扣库存 + MySQL 占码与扣款见 database/points_mall_*.go、service/points_mall_service.go、handler/points_mall_handler.go。
//  4 工程横切：JWT middleware/auth.go、限流 middleware/rate_limit.go、布隆与读穿透 cache/article_guard.go、
//    雪花 tool/snowflake_module.go、Compose 见仓库 docker-compose*。
//  5 实时通知：WebSocket 先推、离线异步落库见 handler/notification_handler.go（mq.NotifySink）；
//    Kafka notification.store 批量落库见 handler/notification_store_handler.go；notification.push 仅兼容旧/外部生产者。
package router

import (
	"net/http"

	"github.com/gao66666/GoBlog/database"
	"github.com/gao66666/GoBlog/handler"
	"github.com/gao66666/GoBlog/logger"
	"github.com/gao66666/GoBlog/middleware"
	"github.com/gao66666/GoBlog/mq"
	"github.com/gao66666/GoBlog/service"
	"github.com/gao66666/GoBlog/setting"
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// App 结构体用来持有所有的 Handler，方便路由挂载
type App struct {
	UserHandler              *handler.UserHandler
	ArticleHandler           *handler.ArticleHandler
	CommentHandler           *handler.CommentHandler
	FollowHandler            *handler.FollowHandler
	NotificationHandler      *handler.NotificationHandler
	NotificationStoreHandler *handler.NotificationStoreHandler
	SearchHandler            *handler.SearchHandler
	DMHandler                *handler.DMHandler
	TopicHandler             *handler.TopicHandler
	GameHandler              *handler.GameHandler
	PointsHandler            *handler.PointsHandler
	PointsMallHandler        *handler.PointsMallHandler
	PointsSvc                *service.PointsService
	InternalHandler          *handler.InternalHandler
}

// StartPeriodicJobs 启动与 Kafka 无关的后台周期任务（例如通知批量落库 ticker、积分对账）。
func (a *App) StartPeriodicJobs() {
	if a.NotificationStoreHandler != nil {
		a.NotificationStoreHandler.StartFlushTicks()
	}
	if a.PointsSvc != nil {
		a.PointsSvc.StartReconcileTicker()
	}
	zap.L().Info("后台周期任务已启动（通知落库 flush、积分对账）")
}

// StartWorkers 仅注册 Kafka 消费者（依赖 mq.InitKafka 已成功）。
func (a *App) StartWorkers() {
	if err := mq.StartKafkaConsumers(a.NotificationHandler, a.NotificationStoreHandler, a.SearchHandler, a.PointsHandler); err != nil {
		zap.L().Warn("Kafka workers not started", zap.Error(err))
		return
	}
	zap.L().Info("Kafka Worker 启动成功，正在监听【通知】【通知落库】【搜索同步】【积分入账 points.earn / DLQ points.earn.dlq】Topic...")
}

// SetupApp 负责依赖注入的组装过程
func SetupApp(db *gorm.DB, rdb *redis.Client) *App {
	// 1. Repository 层 (MySQL & Redis)
	userRepo := database.NewUserRepository(db)
	userRedis := database.NewRedisUserRepository(rdb)
	articleRepo := database.NewArticleRepository(db)
	articleRedis := database.NewRedisArticleRepository(rdb)
	commentRepo := database.NewCommentRepository(db)
	commentRedis := database.NewRedisCommentRepository(rdb)
	notificationRepo := database.NewNotificationRepository(db)
	notificationRedis := database.NewRedisNotificationRepository(rdb)
	followRepo := database.NewFollowRepository(db)
	followRedis := database.NewRedisFollowRepository(rdb)
	dmRepo := database.NewDMRepository(db)
	dmRedis := database.NewRedisDMRepository(rdb)
	topicRepo := database.NewTopicRepository(db)
	topicRedis := database.NewRedisTopicRepository(rdb)
	gameRepo := database.NewGameRepository(db)
	gameStoreRepo := database.NewGameStoreRepository(db)
	gameRedis := database.NewRedisGameRepository(rdb)

	pointsRepo := database.NewPointsRepository(db)
	pointsRedis := database.NewRedisPointsRepository(rdb)
	pointsSvc := service.NewPointsService(db, pointsRepo, pointsRedis)
	pointsHandler := handler.NewPointsHandler(pointsSvc)

	mallRepo := database.NewPointsMallRepository(db)
	mallRedis := database.NewRedisPointsMallRepository(rdb)
	mallSvc := service.NewPointsMallService(mallRepo, mallRedis, pointsRepo, pointsRedis)
	mallHandler := handler.NewPointsMallHandler(mallSvc)

	if err := userRepo.InitTable(); err != nil {
		zap.L().Warn("用户表初始化失败", zap.Error(err))
	}
	if err := articleRepo.InitTable(); err != nil {
		zap.L().Warn("文章表初始化失败", zap.Error(err))
	}
	if err := commentRepo.InitTable(); err != nil {
		zap.L().Warn("评论表初始化失败", zap.Error(err))
	}
	if err := notificationRepo.InitTable(); err != nil {
		zap.L().Warn("通知表初始化失败", zap.Error(err))
	}
	if err := followRepo.InitTable(); err != nil {
		zap.L().Warn("关注表初始化失败", zap.Error(err))
	}
	if err := dmRepo.InitTable(); err != nil {
		zap.L().Warn("私信表初始化失败", zap.Error(err))
	}
	if err := topicRepo.InitTable(); err != nil {
		zap.L().Warn("话题表初始化失败", zap.Error(err))
	}
	if err := gameRepo.InitTable(); err != nil {
		zap.L().Warn("游戏相关表初始化失败", zap.Error(err))
	}
	if err := gameStoreRepo.InitTable(); err != nil {
		zap.L().Warn("游戏库库存表初始化失败", zap.Error(err))
	}
	if err := pointsRepo.InitTable(); err != nil {
		zap.L().Warn("积分表初始化失败", zap.Error(err))
	}
	if err := mallRepo.InitTable(); err != nil {
		zap.L().Warn("积分商城表初始化失败", zap.Error(err))
	} else {
		mallSvc.InitStockCache()
	}

	// 2. Service 层
	userSvc := service.NewUserService(userRepo, userRedis, followRepo)
	articleSvc := service.NewArticleService(articleRepo, userRepo, commentRepo, followRepo, articleRedis, topicRepo, gameRepo, pointsSvc, userSvc)
	dmSvc := service.NewDMService(dmRepo, userRepo, dmRedis)
	topicSvc := service.NewTopicService(topicRepo, articleRepo, topicRedis)
	gameSvc := service.NewGameService(gameRepo, gameStoreRepo, gameRedis, topicRepo, userRepo, userRedis)
	searchRedis := database.NewRedisSearchRepository(rdb)
	searchSvc := service.NewSearchService(articleRepo, userRepo, commentRepo, searchRedis)

	// 3. Handler 层（通知 WebSocket 需先于依赖 NotifySink 的 Service 创建）
	notificationHandler := handler.NewNotificationHandler(notificationRepo, notificationRedis)
	notificationStoreHandler := handler.NewNotificationStoreHandler(notificationRepo)
	dmHandler := handler.NewDMHandler(dmSvc, notificationHandler)

	followSvc := service.NewFollowService(followRepo, userRepo, followRedis, topicRepo, userSvc, notificationHandler)
	commentSvc := service.NewCommentService(commentRepo, articleRepo, userRepo, commentRedis, notificationHandler, pointsSvc)
	topicHandler := handler.NewTopicHandler(topicSvc, followSvc)

	return &App{
		UserHandler:              handler.NewUserHandler(userSvc),
		ArticleHandler:           handler.NewArticleHandler(articleSvc),
		CommentHandler:           handler.NewCommentHandler(commentSvc),
		FollowHandler:            handler.NewFollowHandler(followSvc, userSvc),
		NotificationHandler:      notificationHandler,
		NotificationStoreHandler: notificationStoreHandler,
		SearchHandler:            handler.NewSearchHandler(searchSvc),
		DMHandler:                dmHandler,
		TopicHandler:             topicHandler,
		GameHandler:              handler.NewGameHandler(gameSvc, userSvc),
		PointsHandler:            pointsHandler,
		PointsMallHandler:        mallHandler,
		PointsSvc:                pointsSvc,
		InternalHandler:          handler.NewInternalHandler(gameSvc, topicSvc),
	}
}

func setGinMode(mode string) {
	switch mode {
	case "dev":
		gin.SetMode(gin.DebugMode)
	case "release":
		gin.SetMode(gin.ReleaseMode)
	default:
		gin.SetMode(gin.TestMode)
	}
}

func RouterInit(mode string, app *App) *gin.Engine {
	// 1. 设置运行模式
	setGinMode(mode)

	//初始化Gin引擎
	r := gin.New()
	// 页面模板与静态资源（最小前端）
	r.LoadHTMLGlob("web/templates/*.tmpl")
	r.Static("/static", "web/static")
	handler.RegisterVueAssets(r)
	r.GET("/favicon.ico", func(c *gin.Context) {
		c.File("web/static/favicon.svg")
	})

	// 全局中间件：日志和异常恢复
	r.Use(
		middleware.RequestID(),
		logger.GinLogger(),
		logger.GinRecovery(true),
		middleware.SecurityHeaders(),
		middleware.RateLimit(setting.Conf.SecurityConfig.RateLimitPerMinute, setting.Conf.SecurityConfig.RateLimitBurst),
	)
	if setting.Conf.ObservabilityConfig.EnableMetrics {
		r.Use(middleware.Metrics())
		r.GET("/metrics", middleware.MetricsHandler)
	}

	// --- 页面路由 ---
	r.GET("/", handler.HomePage)
	r.GET("/login", handler.LoginPage)
	r.GET("/me", handler.MePage)
	r.GET("/u/:id", handler.UserPage)
	r.GET("/dm", handler.DMPage)
	r.GET("/article/:id", handler.ArticlePage)
	r.GET("/editor", handler.EditorPage)
	r.GET("/topics", handler.TopicsPage)
	r.GET("/topic/:id/discuss", handler.TopicDiscussPage)
	r.GET("/topic/:id", handler.TopicPage)
	r.GET("/game-library", handler.GamesLibraryPage)
	r.GET("/game/:id", handler.GameDetailPage)
	r.GET("/points-mall", handler.PointsMallPage)

	r.GET("/healthz", handler.Healthz)
	r.GET("/readyz", handler.Readyz)

	// --- 公开路由 ---
	r.GET("/ping", func(c *gin.Context) { c.String(http.StatusOK, "pong") })

	v1 := r.Group("/api/v1")
	registerPublicRoutes(v1, app)
	registerProtectedRoutes(v1, app)
	registerInternalRoutes(v1, app)

	// 保留旧前缀，减少升级冲击
	legacyV1 := r.Group("/v1")
	registerProtectedRoutes(legacyV1, app)

	r.NoRoute(func(c *gin.Context) {
		c.JSON(http.StatusNotFound, gin.H{"msg": "404 not found"})
	})

	return r
}

func registerPublicRoutes(g *gin.RouterGroup, app *App) {
	g.POST("/signup", app.UserHandler.SignUpHandle)
	g.POST("/login", app.UserHandler.LoginHandle)
	g.GET("/users/:id", app.UserHandler.GetUserPublic)
	g.GET("/users/:id/online", app.NotificationHandler.GetUserOnlineStatus)
	g.GET("/articles/latest", app.ArticleHandler.GetLatestArticlesPublic)
	g.GET("/articles/leaderboard", app.ArticleHandler.GetArticleLeaderboardPublic)
	g.GET("/articles/list", app.ArticleHandler.GetArticleListPublic)
	g.GET("/articles/:id", app.ArticleHandler.ReadArticleHandle)
	g.GET("/articles/comments", app.CommentHandler.GetArticleComment)
	g.GET("/articles/comments/floor/:root_id", app.CommentHandler.GetCommentDetail)
	g.GET("/search", app.SearchHandler.GlobalSearch)
	g.GET("/ws", app.NotificationHandler.HandleWS)

	g.GET("/topics", app.TopicHandler.GetTopicsPublic)
	g.GET("/topics/:id", app.TopicHandler.GetTopicPublic)
	g.GET("/topics/:id/articles", app.TopicHandler.GetTopicArticlesPublic)
	g.GET("/topics/:id/discussions", app.TopicHandler.GetTopicDiscussionsPublic)
	g.GET("/games/search", app.GameHandler.SearchGames)
	g.GET("/games/:id", app.GameHandler.GetGame)
	g.GET("/games", app.GameHandler.ListGames)
	g.GET("/games/:id/reviews", app.GameHandler.ListReviews)

	// 游戏库公开列表/详情（详情含库存；已登录时含 owned）
	g.GET("/game-reviews/:id", app.GameHandler.GetReview)
	g.GET("/game-reviews/:id/comments", app.GameHandler.ListReviewComments)
	g.GET("/users/:id/points", app.PointsHandler.GetUserWallet)
	g.GET("/users/:id/games", app.GameHandler.GetUserGamePlays)
	g.GET("/points/mall/products/search", app.PointsMallHandler.SearchProducts)
	g.GET("/points/mall/products", app.PointsMallHandler.ListProducts)
	g.GET("/points/mall/products/:id", app.PointsMallHandler.GetProduct)

}

// registerInternalRoutes 后台运维 API：X-Admin-Key，不经 JWT，前端不调用。
func registerInternalRoutes(g *gin.RouterGroup, app *App) {
	if app.InternalHandler == nil {
		return
	}
	internal := g.Group("/internal")
	internal.Use(middleware.AdminAPIKey())
	{
		internal.POST("/games", app.InternalHandler.CreateGameInternal)
		internal.POST("/topics", app.InternalHandler.CreateTopicInternal)
	}
}

func registerProtectedRoutes(g *gin.RouterGroup, app *App) {
	authGroup := g.Group("")
	authGroup.Use(middleware.JWTAuthMiddleware())
	{
		authGroup.GET("/users/me", app.UserHandler.GetMe)
		authGroup.POST("/users/update", app.UserHandler.UpdateUserHandle)
		authGroup.POST("/follow", app.FollowHandler.CreateFollowAuth)
		authGroup.POST("/follow/topic", app.FollowHandler.FollowTopicHandle)
		authGroup.DELETE("/follow/topic", app.FollowHandler.UnfollowTopicHandle)
		authGroup.GET("/follow/topics", app.FollowHandler.ListMyFollowedTopicsHandle)
		authGroup.GET("/follow/users/count", app.FollowHandler.GetMyFollowingUserCount)
		authGroup.GET("/dm/peers", app.DMHandler.ListPeers)
		authGroup.GET("/dm/messages", app.DMHandler.ListMessages)
		authGroup.POST("/dm/messages", app.DMHandler.SendMessage)
		authGroup.GET("/dm/unread_count", app.DMHandler.GetUnreadCount)
		authGroup.POST("/dm/unread_clear", app.DMHandler.ClearUnread)
		authGroup.POST("/dm/conversation/delete", app.DMHandler.DeleteConversation)
		authGroup.POST("/articles", app.ArticleHandler.CreateArticleHandle)
		authGroup.PUT("/articles/:id", app.ArticleHandler.UpdateArticleHandle)
		authGroup.GET("/articles", app.ArticleHandler.GetArticleListHandler)
		authGroup.GET("/articles/following_latest", app.ArticleHandler.GetFollowingLatestArticles)
		authGroup.POST("/articles/like", app.ArticleHandler.LikeArticleHandle)
		authGroup.POST("/articles/comments/like", app.CommentHandler.LikeCommentHandle)
		authGroup.POST("/articles/comments/cy", app.CommentHandler.CreateCYHandle)
		authGroup.GET("/comments/cy", app.CommentHandler.GetMyCYListHandle)
		authGroup.POST("/articles/collect", app.ArticleHandler.CollectArticleHandle)
		authGroup.DELETE("/articles/collect", app.ArticleHandler.UncollectArticleHandle)
		authGroup.GET("/articles/collect", app.ArticleHandler.ListMyCollectionsHandle)
		authGroup.DELETE("/articles/:id", app.ArticleHandler.DeleteArticleHandle)
		authGroup.POST("/articles/comments", app.CommentHandler.CreateComment)
		authGroup.DELETE("/articles/comments/:id", app.CommentHandler.DeleteComment)

		authGroup.POST("/topics", app.TopicHandler.CreateTopicAuth)
		authGroup.POST("/topics/:id/discussions", app.TopicHandler.CreateTopicDiscussionAuth)
		authGroup.DELETE("/topics/:id", app.TopicHandler.DeleteTemporaryTopic)
		authGroup.POST("/games", app.GameHandler.CreateGame)
		authGroup.PUT("/games/:id", app.GameHandler.UpdateGame)
		authGroup.DELETE("/games/:id", app.GameHandler.DeleteGame)
		authGroup.POST("/games/:id/reviews", app.GameHandler.CreateReview)
		authGroup.PUT("/game-reviews/:id", app.GameHandler.UpdateReview)
		authGroup.DELETE("/game-reviews/:id", app.GameHandler.DeleteReview)
		authGroup.POST("/game-reviews/:id/comments", app.GameHandler.CreateReviewComment)
		authGroup.PUT("/game-review-comments/:id", app.GameHandler.UpdateReviewComment)
		authGroup.DELETE("/game-review-comments/:id", app.GameHandler.DeleteReviewComment)

		// 游戏库购买（纯 MySQL 事务：库存 + 扣款 + 流水 + 订单）
		authGroup.POST("/games/:id/purchase", app.GameHandler.PurchaseGame)
		authGroup.GET("/games/orders", app.GameHandler.ListMyGameOrders)

		// 游戏游玩记录
		authGroup.POST("/games/play", app.GameHandler.UpsertMyGamePlay)
		authGroup.GET("/games/play", app.GameHandler.GetMyGamePlays)
		authGroup.DELETE("/games/play/:gameId", app.GameHandler.DeleteMyGamePlay)

		authGroup.GET("/points/wallet", app.PointsHandler.GetMyWallet)
		authGroup.GET("/points/transactions", app.PointsHandler.ListMyTransactions)
		authGroup.POST("/points/checkin", app.PointsHandler.Checkin)
		authGroup.GET("/points/mall/orders", app.PointsMallHandler.ListMyOrders)
		authGroup.POST("/points/mall/redeem", app.PointsMallHandler.Redeem)
	}
}
