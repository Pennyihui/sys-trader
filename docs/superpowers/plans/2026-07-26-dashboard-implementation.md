# SysTrader Dashboard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended)

**Goal:** 可视化监控面板 — FastAPI 后端从现有模块收集数据，React 前端通过 WebSocket 实时展示。

**Architecture:** FastAPI WebSocket 服务作为数据中枢，聚合 Portfolio/MarketData/Guardian 等模块的状态，推送到 React 前端。前后端分离，通过 WebSocket 通信。

**Tech Stack:** FastAPI, Python 3.12+, React 18, Vite, Recharts, TailwindCSS, WebSocket

---

### Task 1: FastAPI 后端 — WebSocket 服务

**Files:**
- Create: `dashboard/server.py`
- Create: `dashboard/__init__.py`
- Create: `dashboard/data_collector.py`
- Create: `tests/test_dashboard.py`

- [ ] **Step 1: 编写后端测试**

`tests/test_dashboard.py`:
```python
import pytest
from unittest.mock import MagicMock, patch
from dashboard.data_collector import DataCollector


class TestDataCollector:
    def setup_method(self):
        self.feed = MagicMock()
        self.feed.get_last_price.return_value = 64000.0
        self.feed.get_mark_price.return_value = 64000.0
        self.feed.buffer.count.return_value = 5
        self.portfolio = MagicMock()
        self.portfolio.total_equity = 10000.0
        self.portfolio.total_margin = 1200.0
        self.portfolio.margin_ratio = 0.12
        self.portfolio.daily_realized_pnl = 50.0
        self.portfolio.current_drawdown = 0.03
        self.portfolio.positions = {}
        self.collector = DataCollector(feed=self.feed, portfolio=self.portfolio)

    def test_collect_returns_all_fields(self):
        data = self.collector.collect()
        assert "equity" in data
        assert "margin_ratio" in data
        assert "daily_pnl" in data
        assert "positions" in data
        assert "prices" in data

    def test_collect_btc_mark_price(self):
        data = self.collector.collect()
        assert data["prices"]["BTCUSDT"]["mark"] == 64000.0

    def test_empty_positions_returns_empty_list(self):
        data = self.collector.collect()
        assert data["positions"] == []

    def test_drawdown_included(self):
        data = self.collector.collect()
        assert "drawdown" in data
```

- [ ] **Step 2: 运行测试验证失败**

```bash
cd D:/Documents/z_python_data_analy/Quent/Sys_trader && python -m pytest tests/test_dashboard.py -v
```
Expected: FAIL

- [ ] **Step 3: 实现 DataCollector**

`dashboard/data_collector.py`:
```python
"""DataCollector — 从各模块聚合数据，供 Dashboard WebSocket 推送。"""

import logging
from typing import Any, Dict, List, Optional
from market_data.feed import MarketDataFeed
from portfolio.tracker import PortfolioTracker

logger = logging.getLogger(__name__)


class DataCollector:
    def __init__(self, feed: MarketDataFeed, portfolio: PortfolioTracker):
        self.feed = feed
        self.portfolio = portfolio

    def collect(self) -> Dict[str, Any]:
        positions = []
        for symbol, pos in self.portfolio.positions.items():
            mark = self.feed.get_mark_price(symbol) or 0.0
            upnl = self.portfolio.unrealized_pnl(symbol, mark)
            positions.append({
                "symbol": symbol,
                "direction": pos.direction,
                "quantity": pos.quantity,
                "entry_price": pos.entry_price,
                "mark_price": round(mark, 2),
                "unrealized_pnl": round(upnl, 2),
            })
        return {
            "equity": round(self.portfolio.total_equity, 2),
            "margin_ratio": round(self.portfolio.margin_ratio, 2),
            "daily_pnl": round(self.portfolio.daily_realized_pnl, 2),
            "drawdown": round(self.portfolio.current_drawdown, 4),
            "position_count": len(positions),
            "positions": positions,
            "prices": self._collect_prices(),
        }

    def _collect_prices(self) -> Dict:
        prices = {}
        for symbol in list(self.portfolio.positions.keys()):
            last = self.feed.get_last_price(symbol)
            mark = self.feed.get_mark_price(symbol)
            if last or mark:
                prices[symbol] = {"last": last, "mark": mark}
        return prices
```

- [ ] **Step 4: 运行测试验证通过**

```bash
cd D:/Documents/z_python_data_analy/Quent/Sys_trader && python -m pytest tests/test_dashboard.py -v
```
Expected: PASS

- [ ] **Step 5: 实现 FastAPI 服务**

`dashboard/server.py`:
```python
"""FastAPI WebSocket 服务 — 实时推送交易系统数据到 Dashboard。"""

import asyncio
import json
import logging
from contextlib import asynccontextmanager
from typing import Optional

import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

from dashboard.data_collector import DataCollector

logger = logging.getLogger(__name__)


class DashboardServer:
    def __init__(self, data_collector: DataCollector, push_interval: float = 1.0):
        self.collector = data_collector
        self.push_interval = push_interval
        self._app: Optional[FastAPI] = None
        self._clients: set[WebSocket] = set()

    def _create_app(self) -> FastAPI:
        @asynccontextmanager
        async def lifespan(app: FastAPI):
            task = asyncio.create_task(self._broadcast_loop())
            yield
            task.cancel()

        app = FastAPI(lifespan=lifespan)

        @app.websocket("/ws")
        async def websocket_endpoint(ws: WebSocket):
            await ws.accept()
            self._clients.add(ws)
            # 立即推送初始数据
            await ws.send_json(self.collector.collect())
            try:
                while True:
                    msg = await ws.receive_text()
                    if msg == "pause":
                        logger.info("[Dashboard] pause requested")
                    elif msg == "resume":
                        logger.info("[Dashboard] resume requested")
                    elif msg == "emergency_stop":
                        logger.info("[Dashboard] EMERGENCY STOP")
            except WebSocketDisconnect:
                pass
            finally:
                self._clients.discard(ws)

        @app.get("/health")
        async def health():
            return {"status": "ok", "clients": len(self._clients)}

        return app

    async def _broadcast_loop(self):
        while True:
            await asyncio.sleep(self.push_interval)
            data = self.collector.collect()
            dead = set()
            for ws in self._clients:
                try:
                    await ws.send_json(data)
                except Exception:
                    dead.add(ws)
            self._clients -= dead

    @property
    def app(self) -> FastAPI:
        if self._app is None:
            self._app = self._create_app()
        return self._app

    def run(self, host: str = "0.0.0.0", port: int = 8000):
        uvicorn.run(self.app, host=host, port=port)
```

- [ ] **Step 6: 运行测试验证通过**

```bash
cd D:/Documents/z_python_data_analy/Quent/Sys_trader && python -m pytest tests/test_dashboard.py -v
```
Expected: PASS (4/4)

- [ ] **Step 7: Commit**

```bash
git add dashboard/ tests/test_dashboard.py
git commit -m "feat: add Dashboard FastAPI backend with WebSocket and DataCollector"
```

---

### Task 2: React 前端

**Files:**
- Create: `dashboard/frontend/package.json`
- Create: `dashboard/frontend/vite.config.ts`
- Create: `dashboard/frontend/tsconfig.json`
- Create: `dashboard/frontend/tsconfig.app.json`
- Create: `dashboard/frontend/index.html`
- Create: `dashboard/frontend/src/main.tsx`
- Create: `dashboard/frontend/src/App.tsx`
- Create: `dashboard/frontend/src/hooks/useWebSocket.ts`
- Create: `dashboard/frontend/src/components/StatusBar.tsx`
- Create: `dashboard/frontend/src/components/MetricCards.tsx`
- Create: `dashboard/frontend/src/components/PositionsTable.tsx`
- Create: `dashboard/frontend/src/components/SignalsList.tsx`
- Create: `dashboard/frontend/src/components/ModuleStatus.tsx`
- Create: `dashboard/frontend/src/components/Controls.tsx`

- [ ] **Step 1: 初始化 Vite + React 项目**

`dashboard/frontend/package.json`:
```json
{
  "name": "systrader-dashboard",
  "private": true,
  "version": "0.1.0",
  "type": "module",
  "scripts": {
    "dev": "vite",
    "build": "tsc -b && vite build",
    "preview": "vite preview"
  },
  "dependencies": {
    "react": "^18.3.0",
    "react-dom": "^18.3.0",
    "recharts": "^2.12.0"
  },
  "devDependencies": {
    "@types/react": "^18.3.0",
    "@types/react-dom": "^18.3.0",
    "@vitejs/plugin-react": "^4.3.0",
    "autoprefixer": "^10.4.0",
    "postcss": "^8.4.0",
    "tailwindcss": "^3.4.0",
    "typescript": "^5.5.0",
    "vite": "^5.4.0"
  }
}
```

- [ ] **Step 2: 创建 Vite 配置**

`dashboard/frontend/vite.config.ts`:
```typescript
import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      '/ws': { target: 'ws://localhost:8000', ws: true },
      '/health': 'http://localhost:8000',
    },
  },
});
```

- [ ] **Step 3: 创建 useWebSocket hook**

`dashboard/frontend/src/hooks/useWebSocket.ts`:
```typescript
import { useEffect, useRef, useState } from 'react';

export interface DashboardData {
  equity: number;
  margin_ratio: number;
  daily_pnl: number;
  drawdown: number;
  position_count: number;
  positions: any[];
  prices: Record<string, any>;
}

export function useWebSocket() {
  const [data, setData] = useState<DashboardData | null>(null);
  const [connected, setConnected] = useState(false);
  const ws = useRef<WebSocket | null>(null);

  useEffect(() => {
    const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
    const url = `${proto}//${location.host}/ws`;
    ws.current = new WebSocket(url);
    ws.current.onopen = () => setConnected(true);
    ws.current.onclose = () => setConnected(false);
    ws.current.onmessage = (e) => setData(JSON.parse(e.data));
    return () => ws.current?.close();
  }, []);

  const send = (msg: string) => ws.current?.send(msg);

  return { data, connected, send };
}
```

- [ ] **Step 4: 创建 App 组件**

`dashboard/frontend/src/App.tsx`:
```typescript
import { useWebSocket } from './hooks/useWebSocket';
import { StatusBar } from './components/StatusBar';
import { MetricCards } from './components/MetricCards';
import { PositionsTable } from './components/PositionsTable';
import { SignalsList } from './components/SignalsList';
import { ModuleStatus } from './components/ModuleStatus';
import { Controls } from './components/Controls';

export default function App() {
  const { data, connected, send } = useWebSocket();

  return (
    <div className="min-h-screen bg-gray-900 text-gray-100 p-4">
      <StatusBar connected={connected} />
      {data ? (
        <>
          <MetricCards
            equity={data.equity}
            positionCount={data.position_count}
            marginRatio={data.margin_ratio}
            dailyPnl={data.daily_pnl}
          />
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 mt-4">
            <PositionsTable positions={data.positions} />
            <SignalsList />
          </div>
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 mt-4">
            <ModuleStatus />
            <Controls onCommand={send} />
          </div>
        </>
      ) : (
        <div className="flex items-center justify-center h-64 text-gray-500">
          等待数据连接...
        </div>
      )}
    </div>
  );
}
```

- [ ] **Step 5: 创建各组件**

`dashboard/frontend/src/components/StatusBar.tsx`:
```typescript
export function StatusBar({ connected }: { connected: boolean }) {
  return (
    <div className="flex items-center justify-between mb-4 pb-2 border-b border-gray-700">
      <h1 className="text-xl font-bold">SysTrader Dashboard</h1>
      <div className="flex items-center gap-2">
        <span className={`w-3 h-3 rounded-full ${connected ? 'bg-green-500' : 'bg-red-500'}`} />
        <span className="text-sm text-gray-400">{connected ? 'CONNECTED' : 'DISCONNECTED'}</span>
      </div>
    </div>
  );
}
```

`dashboard/frontend/src/components/MetricCards.tsx`:
```typescript
interface Props {
  equity: number; positionCount: number; marginRatio: number; dailyPnl: number;
}
export function MetricCards({ equity, positionCount, marginRatio, dailyPnl }: Props) {
  const cards = [
    { label: '账户权益', value: `$${equity.toLocaleString()}`, color: '' },
    { label: '持仓数', value: `${positionCount}`, color: '' },
    { label: '保证金率', value: `${(marginRatio * 100).toFixed(1)}%`, color: '' },
    { label: '今日盈亏', value: `${dailyPnl >= 0 ? '+' : ''}$${dailyPnl.toFixed(2)}`,
      color: dailyPnl >= 0 ? 'text-green-400' : 'text-red-400' },
  ];
  return (
    <div className="grid grid-cols-4 gap-4">
      {cards.map((c) => (
        <div key={c.label} className="bg-gray-800 rounded-lg p-4">
          <div className="text-sm text-gray-400">{c.label}</div>
          <div className={`text-2xl font-bold ${c.color}`}>{c.value}</div>
        </div>
      ))}
    </div>
  );
}
```

`dashboard/frontend/src/components/PositionsTable.tsx`:
```typescript
export function PositionsTable({ positions }: { positions: any[] }) {
  if (!positions.length) return (
    <div className="bg-gray-800 rounded-lg p-4">
      <h2 className="text-sm font-semibold mb-2 text-gray-400">当前持仓</h2>
      <div className="text-gray-500 text-sm">无持仓</div>
    </div>
  );
  return (
    <div className="bg-gray-800 rounded-lg p-4">
      <h2 className="text-sm font-semibold mb-2 text-gray-400">当前持仓</h2>
      <table className="w-full text-sm">
        <thead>
          <tr className="text-gray-500 border-b border-gray-700">
            <th className="text-left py-1">标的</th>
            <th className="text-right py-1">方向</th>
            <th className="text-right py-1">数量</th>
            <th className="text-right py-1">入场</th>
            <th className="text-right py-1">标记价</th>
            <th className="text-right py-1">未实现盈亏</th>
          </tr>
        </thead>
        <tbody>
          {positions.map((p: any) => (
            <tr key={p.symbol} className="border-b border-gray-700/50">
              <td className="py-2">{p.symbol}</td>
              <td className={`text-right ${p.direction === 'LONG' ? 'text-green-400' : 'text-red-400'}`}>
                {p.direction}
              </td>
              <td className="text-right">{p.quantity}</td>
              <td className="text-right">{p.entry_price?.toLocaleString()}</td>
              <td className="text-right">{p.mark_price?.toLocaleString()}</td>
              <td className={`text-right ${p.unrealized_pnl >= 0 ? 'text-green-400' : 'text-red-400'}`}>
                {p.unrealized_pnl >= 0 ? '+' : ''}{p.unrealized_pnl?.toFixed(2)}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
```

`dashboard/frontend/src/components/SignalsList.tsx`:
```typescript
export function SignalsList() {
  // 未来接入 Signal Engine 后显示实时信号
  return (
    <div className="bg-gray-800 rounded-lg p-4">
      <h2 className="text-sm font-semibold mb-2 text-gray-400">最近信号</h2>
      <div className="text-gray-500 text-sm">等待信号引擎接入...</div>
    </div>
  );
}
```

`dashboard/frontend/src/components/ModuleStatus.tsx`:
```typescript
const modules = [
  'Market Data', 'Scheduler', 'Signal Engine', 'Risk Manager',
  'Execution', 'Portfolio', 'Guardian', 'Monitor',
];
export function ModuleStatus() {
  return (
    <div className="bg-gray-800 rounded-lg p-4">
      <h2 className="text-sm font-semibold mb-2 text-gray-400">模块状态</h2>
      <div className="grid grid-cols-2 gap-2">
        {modules.map((m) => (
          <div key={m} className="flex items-center gap-2 text-sm">
            <span className="w-2 h-2 rounded-full bg-green-500" />
            {m}
          </div>
        ))}
      </div>
    </div>
  );
}
```

`dashboard/frontend/src/components/Controls.tsx`:
```typescript
export function Controls({ onCommand }: { onCommand: (cmd: string) => void }) {
  return (
    <div className="bg-gray-800 rounded-lg p-4">
      <h2 className="text-sm font-semibold mb-2 text-gray-400">控制</h2>
      <div className="flex gap-2">
        <button onClick={() => onCommand('pause')} className="px-3 py-1.5 rounded bg-yellow-600 hover:bg-yellow-500 text-sm">
          暂停
        </button>
        <button onClick={() => onCommand('resume')} className="px-3 py-1.5 rounded bg-green-600 hover:bg-green-500 text-sm">
          恢复
        </button>
        <button onClick={() => onCommand('emergency_stop')} className="px-3 py-1.5 rounded bg-red-600 hover:bg-red-500 text-sm">
          紧急平仓
        </button>
      </div>
    </div>
  );
}
```

`dashboard/frontend/src/main.tsx`:
```typescript
import React from 'react';
import ReactDOM from 'react-dom/client';
import App from './App';
import './index.css';

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode><App /></React.StrictMode>,
);
```

`dashboard/frontend/src/index.css`:
```css
@tailwind base;
@tailwind components;
@tailwind utilities;
```

`dashboard/frontend/index.html`:
```html
<!DOCTYPE html>
<html lang="zh">
<head><meta charset="UTF-8" /><meta name="viewport" content="width=device-width, initial-scale=1.0" /><title>SysTrader Dashboard</title></head>
<body class="bg-gray-900"><div id="root"></div><script type="module" src="/src/main.tsx"></script></body>
</html>
```

`dashboard/frontend/tailwind.config.js`:
```javascript
/** @type {import('tailwindcss').Config} */
export default { content: ['./index.html', './src/**/*.{ts,tsx}'], theme: { extend: {} }, plugins: [], }
```

`dashboard/frontend/postcss.config.js`:
```javascript
export default { plugins: { tailwindcss: {}, autoprefixer: {} } }
```

- [ ] **Step 6: 安装依赖并验证编译**

```bash
cd D:/Documents/z_python_data_analy/Quent/Sys_trader/dashboard/frontend && npm install && npx tsc --noEmit && echo "前端编译通过"
```

- [ ] **Step 7: 运行全量测试 + 提交**

```bash
cd D:/Documents/z_python_data_analy/Quent/Sys_trader && python -m pytest tests/ -v --tb=short
```

```bash
git add dashboard/frontend/
git commit -m "feat: add React dashboard frontend with WebSocket connection"
```
