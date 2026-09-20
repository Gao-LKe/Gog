# GameBog

基于 **Gin + GORM + Redis + NSQ + Kafka + Elasticsearch** 的社区和商城后端，含 Vue 前端。

网页链接地址:http://49.235.172.68:8084

## 功能概览

- 用户注册登录、JWT 鉴权、关注与粉丝
- 文章发布/编辑/搜索，点赞与阅读异步统计（NSQ + Redis + MySQL）
- 评论、私信、话题与游戏库
- 通知：Kafka 异步 + WebSocket 实时推送，离线补偿
- 积分系统 & **积分商城**：赚取积分 → 兑换商品（Redis Lua 扣库存 + MySQL 占码）
- 全文检索（Elasticsearch），热门与排行榜（Redis ZSet）

## 技术栈

| 层        | 技术                      |
| --------- | ------------------------- |
| 后端      | Go、Gin、GORM、MySQL      |
| 缓存/队列 | Redis、NSQ、Kafka         |
| 搜索      | Elasticsearch             |
| 前端      | Vue 3、Vite、Element Plus |
| 部署      | Docker Compose、Nginx     |

## 目录结构

`bootstrap` · `router` · `handler` · `service` · `database` · `mq` · `middleware` · `setting` · `webapp`

## 服务与端口

| 服务                | 地址                  |
| ------------------- | --------------------- |
| 后端 API            | http://127.0.0.1:8084 |
| 前端（Vite 热更新） | http://127.0.0.1:5173 |
| NSQ Admin           | http://127.0.0.1:4171 |
| Redis               | 127.0.0.1:6379        |
| MySQL               | 127.0.0.1:3306        |
| Kafka               | 127.0.0.1:9092        |

## 快速启动

**启动依赖服务：**

```bash
docker compose up -d
```

**本机启动 Go 后端：**

```bash
go run .
```

**全栈开发（Go + Vue 热更新，无需本机 Node）：**

```bash
docker compose --profile dev up -d
docker compose --profile dev up -d --force-recreate dev
```

- 前端：http://127.0.0.1:5173  
- 后端：http://127.0.0.1:8084  

新增/修改 Go 接口后需重建 dev 容器：`docker compose --profile dev up -d --force-recreate dev`

**生产/容器一键：**

```bash
docker compose --profile app up -d --build
```

健康检查：`GET /healthz`、`GET /readyz`

## 配置

主配置：`setting/common.yaml`。生产环境务必覆盖 `auth.jwt_secret`。

常用项：`mq.nsq.enabled`、`mq.kafka` 与搜索配置。

搜索服务默认关闭，如需启用 Elasticsearch，请参考 [docker-compose.yml](docker-compose.yml) 中注释的 elasticsearch 服务。

API 前缀 `/api/v1`，鉴权头 `Authorization: Bearer <token>`。

### 后台内部 API（不经前端）

配置 `ADMIN_API_SECRET`（或 `security.admin_api_secret`），请求头 `X-Admin-Key: <secret>`：

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/api/v1/internal/games` | 上新游戏，自动创建并绑定「游戏:名称」话题；返回 `game_id`、`topic_id` |
| POST | `/api/v1/internal/topics` | 创建长期话题；body `{ "name": "话题名" }` |

## 测试

```bash
go test ./...
go vet ./...
```

数据迁移见 `database/migrations/README.md`。
