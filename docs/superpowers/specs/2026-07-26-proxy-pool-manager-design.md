# Proxy Pool Manager 代理池管理系统设计

## 概述

为交易系统设计一个独立的代理池管理系统，解决 Clash 订阅更新导致可用节点丢失的问题。

## 问题

Clash 的订阅更新机制是"全部替换"：
- 订阅更新 → 下载新文件 → 覆盖旧文件
- 旧文件里的所有节点被删除
- 包括可用的旧节点
- 新节点可能还未测速，不可用
- 导致 `auto-failover` 组出现空窗期

## 需求

1. 订阅更新时，旧可用节点**保留**
2. 新节点从订阅 URL 获取，**加入**池子
3. 所有节点**持续测速**，标记可用/不可用
4. 连续失败超过 N 天的节点**自动清理**
5. 自动生成 `auto-failover` 组的 Clash 配置
6. 独立运行，不影响交易系统

## 架构设计

```
┌─────────────────────────────────────────────────────────────────┐
│                     Proxy Pool Manager                          │
│                                                                 │
│  订阅 URL ──→ ① download.py ──→ 解析节点                         │
│                                     │                           │
│  本地数据库 ←── ② merge.py ←────────┘                           │
│  proxy_pool.json     │ 合并新旧节点                               │
│  ├─ 节点名称          │ 旧可用节点保留                              │
│  ├─ 类型(hysteria2)   │ 新节点加入                                  │
│  ├─ 服务器地址         │ 死节点标记                                   │
│  ├─ 端口              │                                           │
│  ├─ 密码              │                                           │
│  ├─ 可用状态 ✅/❌     │                                           │
│  ├─ 最后测试时间       │                                           │
│  └─ 失败次数           │                                           │
│                      │                                           │
│  ③ health_checker.py ──→ 每60秒测速所有节点                         │
│                      │  可用 → 标记 ✅                              │
│                      │  不可用 → 标记 ❌, 失败次数+1                  │
│                      │  失败 > 7天 → 清理                           │
│                      │                                           │
│  ④ config_generator.py ──→ 生成 auto-failover 组配置                │
│                           只包含可用节点 + 新节点                     │
│                           → 更新 Clash 配置文件                      │
│                           → 重启核心                                │
└─────────────────────────────────────────────────────────────────┘
```

## 模块设计

### 1. 本地数据库 (`proxy_pool.json`)

```json
{
  "version": 1,
  "last_updated": "2026-07-26T23:00:00Z",
  "proxies": [
    {
      "name": "hysteria2-xxx",
      "type": "hysteria2",
      "server": "q.baoge.me",
      "port": 2328,
      "password": "xxx",
      "sni": "q.baoge.me",
      "skip_cert_verify": true,
      "healthy": true,
      "last_checked": "2026-07-26T22:59:00Z",
      "fail_count": 0,
      "added_at": "2026-07-26T20:00:00Z",
      "source": "subscription"
    },
    {
      "name": "shadowsocks-xxx",
      "type": "ss",
      "server": "xxx",
      "port": 443,
      "cipher": "chacha20-ietf-poly1305",
      "password": "xxx",
      "healthy": false,
      "last_checked": "2026-07-26T22:58:00Z",
      "fail_count": 5,
      "added_at": "2026-07-20T10:00:00Z",
      "source": "subscription"
    }
  ],
  "proxy_groups": {
    "auto-failover": {
      "type": "load-balance",
      "strategy": "round-robin",
      "url": "http://www.gstatic.com/generate_204",
      "interval": 60
    }
  }
}
```

### 2. 订阅下载器 (`subscription.py`)

```
功能：
  - 从订阅 URL 下载 Clash 配置文件
  - 解析 proxies 部分，提取所有节点
  - 返回节点列表

输入：订阅 URL
输出：节点列表 [{name, type, server, port, ...}]
```

### 3. 合并引擎 (`merge.py`)

```
功能：
  - 读取本地数据库
  - 读取新节点列表
  - 合并策略：
    ┌──────────────────────────────────────────────┐
    │ 新节点有，旧节点没有 → 加入池子（标记为新）    │
    │ 新旧都有 → 保留旧节点状态（可用/不可用）       │
    │ 旧节点有，新节点没有 → 保留（标记为"残留"）    │
    │ 残留节点连续失败 > 7天 → 清理                 │
    └──────────────────────────────────────────────┘

输入：本地数据库, 新节点列表
输出：更新后的本地数据库
```

### 4. 健康检查器 (`health_checker.py`)

```
功能：
  - 遍历池子里所有节点
  - 对每个节点发起 TCP 连接测试
  - 可用 → healthy=true, fail_count=0
  - 不可用 → healthy=false, fail_count+=1
  - fail_count > 10080（7天×24h×60次/小时）→ 清理

输入：本地数据库
输出：更新后的本地数据库
```

### 5. 配置生成器 (`config_generator.py`)

```
功能：
  - 读取本地数据库
  - 筛选 healthy=true 的节点
  - 生成 Clash 格式的 proxies 和 proxy-groups 配置
  - 写入 Clash 配置文件
  - 重启 Clash 核心

生成的配置：
  proxies:
    - {name: xxx, type: hysteria2, ...}
    - {name: xxx, type: ss, ...}
  
  proxy-groups:
    - name: auto-failover
      type: load-balance
      strategy: round-robin
      proxies:
        - xxx
        - xxx
      url: http://www.gstatic.com/generate_204
      interval: 60
```

## 与 Clash 的集成

### 方案一：直接修改 Clash 配置文件

```
config_generator.py 直接修改 clash-verge.yaml 的 proxies 和 proxy-groups 部分
然后重启 Clash 核心
```

- 优点：简单直接
- 缺点：Clash 重启时有短暂中断

### 方案二：通过 Clash API 热更新

```
config_generator.py 通过 Clash API (127.0.0.1:9097) 更新配置
不需要重启核心
```

- 优点：零中断
- 缺点：需要 API 可用

## 运行方式

### 手动运行

```bash
python tools/proxy_pool.py --update    # 更新订阅 + 合并
python tools/proxy_pool.py --health    # 测速所有节点
python tools/proxy_pool.py --generate  # 生成配置
```

### 定时运行（推荐）

```
每6小时: --update     # 更新订阅 + 合并新节点
每60秒:  --health     # 测速所有节点
更新后:  --generate   # 生成配置
```

## 文件结构

```
Sys_trader/tools/
├── proxy_pool.py          # 主入口
├── proxy_pool.json        # 本地数据库
├── subscription.py        # 订阅下载器
├── merge.py               # 合并引擎
├── health_checker.py      # 健康检查器
├── config_generator.py    # 配置生成器
└── requirements.txt       # 依赖
```

## 实施计划

1. 实现 `proxy_pool.json` 数据结构和读写
2. 实现 `subscription.py` 订阅下载和解析
3. 实现 `merge.py` 合并引擎
4. 实现 `health_checker.py` 健康检查
5. 实现 `config_generator.py` 配置生成
6. 实现 `proxy_pool.py` 主入口
7. 集成到 Clash 配置
8. 测试验证