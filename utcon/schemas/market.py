from typing import List, Optional

from pydantic import BaseModel, Field


class MarketQuoteUpsertRequest(BaseModel):
    symbol_code: str
    as_of_ts: int

    last_trade_price: Optional[float] = None
    last_trade_ts: Optional[int] = None

    best_bid: Optional[float] = None
    best_bid_ts: Optional[int] = None

    best_ask: Optional[float] = None
    best_ask_ts: Optional[int] = None

    mid_price: Optional[float] = None
    mark_price: Optional[float] = None

    previous_close: Optional[float] = None
    session_open: Optional[float] = None


class MarketQuoteSampleUpsertRequest(BaseModel):
    symbol_code: str
    sample_ts: int

    last_trade_price: Optional[float] = None
    best_bid: Optional[float] = None
    best_ask: Optional[float] = None
    mid_price: Optional[float] = None
    microprice: Optional[float] = None
    mark_price: Optional[float] = None

    bid_liquidity: float = 0.0
    ask_liquidity: float = 0.0

    trade_count_delta: int = 0
    trade_volume_delta: float = 0.0

    source_trade_count: int = 0
    source_shop_count: int = 0
    is_synthetic: bool = False


class MarketCandleUpsertItem(BaseModel):
    bucket_start_ts: int
    bucket_end_ts: int

    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: Optional[float] = None

    vwap: Optional[float] = None
    median: Optional[float] = None

    trade_volume: float = 0.0
    trade_count: int = 0

    buy_volume: float = 0.0
    sell_volume: float = 0.0

    best_bid: Optional[float] = None
    best_ask: Optional[float] = None
    midpoint: Optional[float] = None

    source_trade_count: int = 0
    source_shop_count: int = 0


class MarketCandlesUpsertRequest(BaseModel):
    symbol_code: str
    interval_key: str
    candles: List[MarketCandleUpsertItem] = Field(default_factory=list)