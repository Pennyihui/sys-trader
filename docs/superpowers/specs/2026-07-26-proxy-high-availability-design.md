# Sys_trader 代理高可用方案设计

## 概述

为交易系统设计零中断的代理方案，通过 4 条并行 WebSocket 连接 + `round-robin` 负载均衡策略，实现任意节点故障时毫秒级切换，数据零丢失。

## 当前问题

- feed.py 只有 1 条 WebSocket 连接
- 节点故障时连接中断，需等待 5 秒重连
- 重连期间行情数据丢失

## 架构设计

### 代理组结构

```
auto 组 (load-balance, consistent-hashing)   ← 普通浏览用，IP 稳定
  └── 所有节点

auto-failover 组 (load-balance, round-robin)  ← 交易系统用，4 连接
  └── 所有节点（通过引用 auto 组）
```

### 规则

```
DOMAIN-SUFFIX,fstream.binance.com → auto-failover  ← 交易数据
GEOSITE,geolocation-!cn           → auto           ← 其他网站
MATCH                             → auto           ← 兜底
```

### 4 连接工作机制

```
正常状态：
  连接1 → auto-failover → 节点A (主)
  连接2 → auto-failover → 节点B (热备)
  连接3 → auto-failover → 节点C (热备)
  连接4 → auto-failover → 节点D (热备)
  ↑ 主连接处理数据，备用连接保持在线

节点A 故障：
  连接1 断开 → 连接2 立即接管为主连接 → 零中断
  → 自动建立新连接5补充到池中作为新的热备
  → 新连接走节点E
```

### 4 连接 vs 2 连接

| 功能 | 2 连接 | 4 连接 |
|------|--------|--------|
| 同时故障容忍 | 1 个节点 | 3 个节点 |
| 热备恢复后 | 只剩 1 条 | 还剩 3 条 |
| 连续故障容错 | 再断就全断 | 还能再扛 2 次 |

## 涉及文件

| 文件 | 改动 |
|------|------|
| `profiles/RUMdLprPCDND.yaml` | 添加 `auto-failover` 组 (round-robin) |
| `profiles/RixNK1q4hAMS.yaml` | 添加 `auto-failover` 组 (round-robin) |
| `profiles/Merge.yaml` | 添加 `fstream.binance.com → auto-failover` 规则 |
| `market_data/feed.py` | 4 连接 + 热备份切换逻辑 |

## feed.py 改造要点

- `__init__` 新增 `redundant_connections: int = 4` 参数
- `start()` 启动 4 条 WebSocket 连接
- 每条连接独立运行，独立重连
- 主连接处理数据，备用连接只保持在线
- 主连接断开时，自动从备用连接中选一条接管
- 补充新连接维持池大小
- 数据去重（同一时刻只有一条连接的数据被处理）

## 实施步骤

1. 修改订阅配置文件，添加 `auto-failover` 组
2. 修改 Merge.yaml，添加 `fstream.binance.com → auto-failover` 规则
3. 修改 feed.py，实现 4 连接高可用
4. 重启 Clash 和交易系统验证