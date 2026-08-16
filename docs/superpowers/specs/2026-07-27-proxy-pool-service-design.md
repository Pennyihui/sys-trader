# Proxy Pool Service 代理池服务设计

## 概述

将 Proxy Pool Manager 改造为独立的 Windows 服务，通过 HTTP API 暴露状态，与 Dashboard 集成。

## 架构

```
┌──────────────────────────────────────────────────────────────┐
│                  Windows Service (nssm)                      │
│  ProxyPoolService.exe → proxy_pool.py --service              │
│                                                              │
│  ┌──────────────────────┐   ┌────────────────────────────┐   │
│  │  Watch Loop           │   │  HTTP API Server (8765)    │   │
│  │  ├── 每6小时更新订阅   │   │  ├── GET /status          │   │
│  │  ├── 每60秒测速       │   │  ├── GET /proxies          │   │
│  │  └── 更新后生成配置   │   │  ├── GET /health           │   │
│  └──────────────────────┘   │  └── GET /metrics           │   │
│                             └────────────────────────────┘   │
│  ┌──────────────────────────────────────────────────────┐    │
│  │  JSON 数据库 (proxy_pool.json)                        │    │
│  │  └── 持久化节点 + 状态 + 指标                          │    │
│  └──────────────────────────────────────────────────────┘    │
└──────────────────────────────────────────────────────────────┘
           │ HTTP API
           ▼
┌──────────────────────────────────────────────────────────────┐
│  Dashboard                                                   │
│  ├── DataCollector → 轮询 GET /status                        │
│  └── 前端代理池状态卡片                                       │
└──────────────────────────────────────────────────────────────┘
```

## 组件设计

### 1. HTTP API Server

Python 内置 `http.server`，不需要额外依赖。

| 端点 | 方法 | 返回 |
|------|------|------|
| `/status` | GET | 池子统计：总节点、可用、不可用、最后更新 |
| `/proxies` | GET | 所有节点列表及其状态 |
| `/proxies?healthy=true` | GET | 仅可用节点 |
| `/health` | GET | 心跳检测，服务是否存活 |
| `/metrics` | GET | 指标：测速耗时、更新次数、告警 |

### 2. Service Watch Loop

```
启动 → 立即执行一次 --all
     → 进入循环：
       ├── 每60秒 → 健康检查（测速）
       ├── 每6小时 → 更新订阅 + 合并 + 测速 + 生成配置
       └── 异常时 → 等待60秒自动重试
```

### 3. nssm 注册

```bash
nssm install ProxyPoolService "python" "D:\...\proxy_pool.py --service"
nssm set ProxyPoolService AppDirectory "D:\...\tools\proxy_pool"
nssm set ProxyPoolService Start SERVICE_AUTO_START
nssm set ProxyPoolService AppStdout "D:\...\logs\proxy_pool.log"
nssm set ProxyPoolService AppStderr "D:\...\logs\proxy_pool.err"
nssm set ProxyPoolService AppRestartDelay 5000
```

### 4. Dashboard 集成

DataCollector 新增 `collect_proxy_pool()` 方法：

```python
def collect_proxy_pool(self) -> dict:
    """从 Proxy Pool Service 获取状态。"""
    try:
        resp = urllib.request.urlopen("http://127.0.0.1:8765/status", timeout=3)
        return json.loads(resp.read())
    except Exception:
        return {"status": "unavailable", "message": "Proxy Pool Service 未运行"}
```

前端新增代理池状态卡片，显示：
- 服务状态（运行中/已停止）
- 可用节点数 / 总节点数
- 最后更新时间
- 告警信息

## 文件改动

| 文件 | 改动 |
|------|------|
| `tools/proxy_pool/proxy_pool.py` | 添加 `--service` 模式 + HTTP API |
| `tools/proxy_pool/api_server.py` | 新增 HTTP API 服务 |
| `dashboard/data_collector.py` | 添加 `collect_proxy_pool()` |
| `dashboard/server.py` | 注册代理池数据路由 |
| 安装脚本 | `nssm install` 命令 |

## 实施步骤

1. 实现 `api_server.py` HTTP API
2. 改造 `proxy_pool.py`，添加 `--service` 模式
3. 安装 nssm 并注册服务
4. 集成到 Dashboard
5. 测试验证