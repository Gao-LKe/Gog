# 商城技术基础架构

这是一个用于继续开发商城项目的最小全栈基础架构，只保留四个技术方向：

- 鉴权：访问令牌、刷新会话和权限边界。
- 实时通信：经过鉴权的 WebSocket 连接与事件推送。
- 支付：支付单状态与幂等入口，支付渠道暂用本地替身。
- 系统防护：限流、并发隔离、超时与熔断边界。

项目暂不包含商品、购物车、搜索、积分、优惠券或社区等业务功能。

## 技术栈

- 后端：Go、Gin、MySQL、Redis、Kafka
- 前端：Vue 3、TypeScript、Vite
- 运行环境：Docker Compose

## 目录

```text
backend/   Go 服务与数据库迁移
frontend/  Vue 3 基础界面
deploy/    本地容器编排与环境示例
docs/      架构说明
```

## 本地启动

```bash
cd deploy
docker compose up --build
```

服务启动后：

- 前端：`http://localhost:8080`
- 后端健康检查：`http://localhost:8081/healthz`

当前阶段只验收基础架构闭环，具体接口以 `docs/architecture.md` 为准。

