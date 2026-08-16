# Proxy Pool Service 实施计划

> **For agentic workers:** 直接在当前会话执行。

**Goal:** 将 Proxy Pool Manager 改造为 Windows 服务，提供 HTTP API，与 Dashboard 集成

**Architecture:** 
- `api_server.py` HTTP API 提供状态查询
- `proxy_pool.py --service` 守护模式 + API 服务
- nssm 注册为 Windows 服务
- DataCollector 轮询 API 集成到 Dashboard

**Tech Stack:** Python 内置 http.server, nssm

---

### Task 1: 实现 HTTP API Server

**Files:**
- Create: `tools/proxy_pool/api_server.py`

实现 4 个端点：
- `GET /status` — 池子统计
- `GET /proxies` — 节点列表
- `GET /health` — 心跳
- `GET /metrics` — 指标

### Task 2: 改造 proxy_pool.py 添加 --service 模式

**Files:**
- Modify: `tools/proxy_pool/proxy_pool.py`

添加 `--service` 模式，启动 Watch Loop + HTTP API Server

### Task 3: 安装 nssm 并注册 Windows 服务

**Files:**
- Create: `tools/proxy_pool/install_service.bat`

下载 nssm，注册 ProxyPoolService 为 Windows 服务

### Task 4: 集成到 Dashboard

**Files:**
- Modify: `dashboard/data_collector.py`
- Modify: `dashboard/server.py`

添加代理池状态采集和 API 路由