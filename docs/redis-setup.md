# Redis 部署（EventBus 依赖）

EventBus / StateStore / Dashboard 依赖 Redis 兼容服务（默认 `localhost:6379`）。
本机日常使用 **Memurai Developer** 直跑；Docker 为等价替代形态（本机不常驻）。

## Windows 直跑（主路径）：Memurai Developer

1. 从 https://memurai.com 下载 Memurai Developer（免费，单实例）
2. 安装后默认监听 localhost:6379，Redis 协议兼容，redis-py 直连无改动
3. **关闭持久化**：Edit 服务配置（或 memurai.conf 不启用 RDB/AOF）——事件流为瞬态数据，丢失无损失，不落盘零 IO
4. 验证: `redis-cli ping` → PONG

## Docker 部署路径（等价形态，本机日常不运行）

`docker-compose.yml` 已含 `redis` 服务；容器内 `REDIS_URL` 指向 redis 容器（`redis://redis:6379`）。
backend 服务注入 `PROXY_HOST=host.docker.internal`（并配 `extra_hosts` 解析），供容器访问宿主机
Clash 代理。**注意：当前代码尚未读取 `PROXY_HOST`**（`dashboard/server.py`、`shared/runner.py`
仍硬编码 `127.0.0.1:7897`）——容器路径的 Binance 访问需后续代码支持；本机 Windows 直跑（Memurai）不受影响。

## 环境变量（可选配置项）

| 变量 | 默认值 | 说明 |
|------|--------|------|
| REDIS_URL | `redis://localhost:6379` | Redis 连接串。Windows 直跑无需设置（默认已指向本机）；Docker 路径由 docker-compose 设为 `redis://redis:6379` |
| DASHBOARD_SYMBOLS | `BTCUSDT,ETHUSDT,SOLUSDT` | Dashboard 行情 feed 订阅的交易对（逗号分隔） |
| DASHBOARD_INSTANCE | `live` | 只消费该 instance 的事件流 |

`config/.env.example` 已含 `REDIS_URL`（复制为 `config/.env` 后可自定义）。
加载优先级：`shared/config_loader.load_env` 用 `load_dotenv(override=False)`，
进程已存在的环境变量（如 docker-compose `environment:` 注入）优先于 .env 文件。
