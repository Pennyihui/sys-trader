"""Binance WebSocket connection pool — stream-to-connection distribution."""

from dataclasses import dataclass
from typing import List


@dataclass
class StreamSpec:
    stream_name: str
    symbol: str
    stream_type: str
    timeframe: str = ""

    @property
    def is_kline(self) -> bool:
        return self.stream_type == "kline"


STREAM_TYPES = [
    {"suffix": "@kline_4h", "type": "kline", "timeframe": "4h"},
    {"suffix": "@kline_1d", "type": "kline", "timeframe": "1d"},
    {"suffix": "@kline_1w", "type": "kline", "timeframe": "1w"},
    {"suffix": "@markprice", "type": "mark_price", "timeframe": ""},
]


def build_stream_list(symbols: List[str]) -> List[StreamSpec]:
    specs = []
    for symbol in symbols:
        sym_lower = symbol.lower()
        for st in STREAM_TYPES:
            specs.append(
                StreamSpec(
                    stream_name=f"{sym_lower}{st['suffix']}",
                    symbol=symbol.upper(),
                    stream_type=st["type"],
                    timeframe=st["timeframe"],
                )
            )
    return specs


class ConnectionPoolConfig:
    def __init__(self, max_pool_size: int = 5):
        self.max_pool_size = max_pool_size

    def effective_pool_size(self, symbols: List[str]) -> int:
        return min(len(symbols), self.max_pool_size)

    def distribute(self, specs: List[StreamSpec]) -> List[List[StreamSpec]]:
        n = max(1, self.effective_pool_size(list(set(s.symbol for s in specs))))
        bins: List[List[StreamSpec]] = [[] for _ in range(n)]
        for i, spec in enumerate(specs):
            bins[i % n].append(spec)
        return bins
