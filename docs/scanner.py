#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Kronos spot scanner v4.0

Objective
- Binance spot USDT pairs only
- 1h chart
- Top 50 liquid names with positive 24h change
- Wyckoff accumulation + Stage 1/2 volume pattern scoring
- Final recommendations capped at 20 symbols
- Long-only, spot-friendly risk control
"""

import json
import math
import os
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests


TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

BASE_URL = "https://api.binance.com"
INTERVAL = "1h"
KLINE_LIMIT = 800

UNIVERSE_SIZE = 50
TOP_RECOMMEND_N = 20
TOP_SIGNAL_N = 8
RISING_ONLY = True
MIN_QV_USD = 20_000_000
MIN_RECOMMEND_SCORE = 55.0
MIN_ACTIONABLE_SCORE = 70.0

EMA_PERIOD = 200
ATR_PERIOD = 14
ADX_PERIOD = 14
CMF_PERIOD = 20
RVOL_LOOKBACK = 24 * 30
RVOL_FALLBACK_LOOKBACK = 48

RR_RATIO = 2.0
RR_RATIO2 = 3.0
BE_TRIGGER = 1.5
EXPIRE_HOURS = 72

ACCUM_BARS_MIN = 48
ACCUM_BARS_MAX = 168
ACCUM_RANGE_MIN = 0.025
ACCUM_RANGE_MAX = 0.10
ADX_FLAT_MAX = 26.0
VOL_BREAKOUT_MULT = 1.8
BODY_RATIO_MIN = 0.50
MAX_STOP_PCT = 9.5
MAX_24H_PUMP_PCT = 18.0
MAX_BREAKOUT_EXTENSION_PCT = 4.0

STATE_PRIORITY = {"TRIGGER": 0, "BREAKOUT": 1, "READY": 2}
ACTIONABLE_STATES = {"TRIGGER", "BREAKOUT"}

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DOCS_DIR = os.path.join(BASE_DIR, "docs")
SIGNALS_FILE = os.path.join(DOCS_DIR, "signals.json")
RECOMMENDATIONS_FILE = os.path.join(DOCS_DIR, "recommendations.json")

STABLE_BASES = {
    "USDT", "USDC", "FDUSD", "TUSD", "BUSD",
    "DAI", "USDP", "USDE", "UST", "USTC",
    "AEUR", "XUSD",
}

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "KronosScanner/4.0"})


def utc_now_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def safe_float(value, default: float = 0.0) -> float:
    try:
        number = float(value)
    except Exception:
        return default
    if math.isnan(number) or math.isinf(number):
        return default
    return number


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def clamp01(value: float) -> float:
    return clamp(value, 0.0, 1.0)


def smart_round(value):
    if value is None or value == 0:
        return value
    digits = max(6, -int(math.floor(math.log10(abs(value)))) + 4)
    return round(value, digits)


def fmt(value) -> str:
    if value is None:
        return "-"
    return f"{float(value):.10f}".rstrip("0").rstrip(".")


def safe_div(numerator: float, denominator: float, default: float = 0.0) -> float:
    numerator = safe_float(numerator, default)
    denominator = safe_float(denominator, 0.0)
    if denominator == 0.0:
        return default
    try:
        return numerator / denominator
    except Exception:
        return default


def pct_change(first: float, last: float) -> float:
    if first in (0, None):
        return 0.0
    return safe_div(last - first, first, 0.0)


def series_return(series: pd.Series) -> float:
    if series.empty:
        return 0.0
    return pct_change(float(series.iloc[0]), float(series.iloc[-1]))


def linear_slope(series: pd.Series) -> float:
    values = [safe_float(v) for v in series.dropna().tolist()]
    n = len(values)
    if n < 2:
        return 0.0
    x_mean = (n - 1) / 2.0
    y_mean = sum(values) / n
    numerator = 0.0
    denominator = 0.0
    for idx, value in enumerate(values):
        dx = idx - x_mean
        numerator += dx * (value - y_mean)
        denominator += dx * dx
    if denominator == 0:
        return 0.0
    return numerator / denominator


def band_score(value: float, low: float, high: float, ideal_low: float, ideal_high: float) -> float:
    if value < low or value > high:
        return 0.0
    if ideal_low <= value <= ideal_high:
        return 1.0
    if value < ideal_low:
        return safe_div(value - low, ideal_low - low, 0.0)
    return safe_div(high - value, high - ideal_high, 0.0)


def cap_score(value: float, low: float, high: float) -> float:
    if high <= low:
        return 1.0 if value >= high else 0.0
    return clamp01((value - low) / (high - low))


def inverse_score(value: float, low: float, high: float) -> float:
    if high <= low:
        return 1.0 if value <= low else 0.0
    return clamp01((high - value) / (high - low))


def qv_to_text(quote_volume: float) -> str:
    if quote_volume >= 1_000_000_000:
        return f"{quote_volume / 1_000_000_000:.2f}B"
    if quote_volume >= 1_000_000:
        return f"{quote_volume / 1_000_000:.1f}M"
    return f"{quote_volume / 1_000:.0f}K"


def symbol_to_display(symbol: str) -> str:
    return symbol[:-4] + "/USDT" if symbol.endswith("USDT") else symbol


def load_json_file(path: str, fallback):
    if not os.path.exists(path):
        return fallback
    try:
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle)
    except Exception:
        return fallback


def save_json_file(path: str, payload):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp_path = path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
    os.replace(tmp_path, path)


def load_signals() -> List[dict]:
    return load_json_file(SIGNALS_FILE, [])


def save_signals(signals: List[dict]):
    save_json_file(SIGNALS_FILE, signals[-1000:])


def save_recommendations(recommendations: List[dict]):
    save_json_file(RECOMMENDATIONS_FILE, recommendations)


def api_get(path: str, params: Optional[dict] = None):
    response = SESSION.get(f"{BASE_URL}{path}", params=params, timeout=20)
    response.raise_for_status()
    return response.json()


def send_telegram(token: str, chat_id: str, signal: dict, is_result: bool = False):
    if not token or not chat_id:
        return

    symbol = signal.get("symbol", "")
    direction = signal.get("direction", "LONG")
    entry = fmt(signal.get("entry"))
    sl = fmt(signal.get("stop_loss"))
    tp1 = fmt(signal.get("take_profit1"))
    tp2 = fmt(signal.get("take_profit2"))
    be = fmt(signal.get("be_target"))
    qv = signal.get("quote_volume", 0.0)
    score = signal.get("total_score")
    state = signal.get("state", "")
    time_text = signal.get("time", "")

    if is_result:
        status = signal.get("status", "")
        result_price = fmt(signal.get("result_price"))
        result_pct = signal.get("result_pct")
        result_pct_text = f"{result_pct:+.2f}%" if result_pct is not None else "-"
        message = (
            f"[RESULT] {symbol}\n"
            f"Direction: {direction} | Status: {status}\n"
            f"Entry: {entry} -> Result: {result_price}\n"
            f"PnL: {result_pct_text}\n"
            f"{time_text}"
        )
    else:
        score_text = f"{score:.1f}" if score is not None else "-"
        comment = signal.get("comment", "")
        breakout_level = fmt(signal.get("breakout_level"))
        entry_zone = signal.get("entry_zone", "-")
        price_change_pct = signal.get("price_change_pct_24h")
        change_text = f"{price_change_pct:+.2f}%" if price_change_pct is not None else "-"
        message = (
            f"[SPOT {state}] {symbol}\n"
            f"Score: {score_text} | 24h: {change_text} | Liquidity: {qv_to_text(qv)}\n"
            f"Entry: {entry}\n"
            f"Entry Zone: {entry_zone}\n"
            f"Stop: {sl}\n"
            f"TP1: {tp1}\n"
            f"TP2: {tp2}\n"
            f"BE: {be}\n"
            f"Breakout: {breakout_level}\n"
            f"Reason: {comment}\n"
            f"{time_text}"
        )

    try:
        requests.get(
            f"https://api.telegram.org/bot{token}/sendMessage",
            params={"chat_id": chat_id, "text": message},
            timeout=10,
        )
    except Exception:
        pass


def fetch_ohlcv(symbol: str, interval: str = INTERVAL, limit: int = KLINE_LIMIT):
    return api_get("/api/v3/klines", {"symbol": symbol, "interval": interval, "limit": limit})


def fetch_last_prices(symbols: List[str]) -> Dict[str, float]:
    prices = api_get("/api/v3/ticker/price")
    wanted = set(symbols)
    return {
        row["symbol"]: safe_float(row["price"])
        for row in prices
        if isinstance(row, dict) and row.get("symbol") in wanted
    }


def get_top_symbols(top_n: int = UNIVERSE_SIZE, rising_only: bool = RISING_ONLY) -> List[dict]:
    info = api_get("/api/v3/exchangeInfo")
    tickers = api_get("/api/v3/ticker/24hr")

    allowed = set()
    for row in info.get("symbols", []):
        if not isinstance(row, dict):
            continue
        if row.get("status") != "TRADING":
            continue
        if row.get("quoteAsset") != "USDT":
            continue
        if row.get("isSpotTradingAllowed") is not True:
            continue
        if row.get("baseAsset", "") in STABLE_BASES:
            continue
        allowed.add(row["symbol"])

    universe = []
    for ticker in tickers:
        if not isinstance(ticker, dict):
            continue
        symbol = ticker.get("symbol")
        if symbol not in allowed:
            continue

        quote_volume = safe_float(ticker.get("quoteVolume"))
        price_change_pct = safe_float(ticker.get("priceChangePercent"))
        last_price = safe_float(ticker.get("lastPrice"))

        if quote_volume < MIN_QV_USD or last_price <= 0:
            continue
        if rising_only and price_change_pct <= 0:
            continue

        universe.append({
            "symbol": symbol,
            "quote_volume": quote_volume,
            "price_change_pct": price_change_pct,
            "last_price": last_price,
        })

    universe.sort(key=lambda item: item["quote_volume"], reverse=True)
    return universe[:top_n]


def to_df(raw) -> pd.DataFrame:
    frame = pd.DataFrame(
        [
            [row[0], safe_float(row[1]), safe_float(row[2]), safe_float(row[3]), safe_float(row[4]), safe_float(row[5])]
            for row in raw
        ],
        columns=["ts", "open", "high", "low", "close", "vol"],
    )
    frame["ts"] = pd.to_datetime(frame["ts"], unit="ms", utc=True)
    return frame


def calc_ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False).mean()


def calc_atr_series(df: pd.DataFrame, period: int = ATR_PERIOD) -> pd.Series:
    prev_close = df["close"].shift(1)
    true_range = pd.concat(
        [
            df["high"] - df["low"],
            (df["high"] - prev_close).abs(),
            (df["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return true_range.ewm(span=period, adjust=False).mean()


def calc_adx_series(df: pd.DataFrame, period: int = ADX_PERIOD) -> pd.Series:
    high = df["high"]
    low = df["low"]
    close = df["close"]

    prev_high = high.shift(1)
    prev_low = low.shift(1)
    prev_close = close.shift(1)

    dm_plus = (high - prev_high).clip(lower=0)
    dm_minus = (prev_low - low).clip(lower=0)
    dm_plus = dm_plus.where(dm_plus > dm_minus, 0.0)
    dm_minus = dm_minus.where(dm_minus > dm_plus, 0.0)

    true_range = pd.concat(
        [
            high - low,
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)

    atr = true_range.ewm(span=period, adjust=False).mean().replace(0, float("nan"))
    di_plus = 100.0 * dm_plus.ewm(span=period, adjust=False).mean() / atr
    di_minus = 100.0 * dm_minus.ewm(span=period, adjust=False).mean() / atr
    dx = 100.0 * (di_plus - di_minus).abs() / (di_plus + di_minus).replace(0, float("nan"))
    return dx.ewm(span=period, adjust=False).mean().fillna(50.0)


def calc_obv(df: pd.DataFrame) -> pd.Series:
    direction = df["close"].diff().fillna(0.0).apply(lambda value: 1 if value > 0 else (-1 if value < 0 else 0))
    return (direction * df["vol"]).cumsum()


def calc_cmf(df: pd.DataFrame, period: int = CMF_PERIOD) -> pd.Series:
    price_range = (df["high"] - df["low"]).replace(0, float("nan"))
    multiplier = ((df["close"] - df["low"]) - (df["high"] - df["close"])) / price_range
    flow_volume = multiplier.fillna(0.0) * df["vol"]
    vol_sum = df["vol"].rolling(period, min_periods=period).sum().replace(0, float("nan"))
    return flow_volume.rolling(period, min_periods=period).sum() / vol_sum


def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["ema200"] = calc_ema(df["close"], EMA_PERIOD)
    df["atr"] = calc_atr_series(df, ATR_PERIOD)
    df["adx"] = calc_adx_series(df, ADX_PERIOD)
    df["obv"] = calc_obv(df)
    df["obv_ema20"] = calc_ema(df["obv"], 20)
    df["cmf"] = calc_cmf(df, CMF_PERIOD)
    df["vol_ma20"] = df["vol"].rolling(20, min_periods=10).mean()

    long_base = df["vol"].rolling(RVOL_LOOKBACK, min_periods=RVOL_FALLBACK_LOOKBACK).mean().shift(1)
    short_base = df["vol"].rolling(RVOL_FALLBACK_LOOKBACK, min_periods=24).mean().shift(1)
    long_rvol = df["vol"] / long_base.replace(0, float("nan"))
    short_rvol = df["vol"] / short_base.replace(0, float("nan"))
    df["rvol"] = long_rvol.fillna(short_rvol).replace([float("inf"), float("-inf")], float("nan")).fillna(1.0)
    return df


def detect_accumulation(df: pd.DataFrame) -> Optional[dict]:
    if len(df) < ACCUM_BARS_MAX + 72:
        return None

    current_price = safe_float(df["close"].iloc[-1])
    body = df.iloc[:-1].copy()
    best = None

    for bars in range(ACCUM_BARS_MIN, ACCUM_BARS_MAX + 1, 4):
        segment = body.iloc[-bars:].copy()
        seg_high = safe_float(segment["high"].max())
        seg_low = safe_float(segment["low"].min())
        avg_price = safe_float(segment["close"].mean())
        if avg_price <= 0 or seg_high <= seg_low:
            continue

        range_ratio = safe_div(seg_high - seg_low, avg_price)
        if not (ACCUM_RANGE_MIN <= range_ratio <= ACCUM_RANGE_MAX):
            continue

        box_range = seg_high - seg_low
        current_pos = safe_div(current_price - seg_low, box_range, 0.0)
        if current_pos < -0.08 or current_pos > 1.18:
            continue

        recent_adx = safe_float(segment["adx"].tail(min(24, bars)).mean())
        if recent_adx > ADX_FLAT_MAX:
            continue

        half = max(1, bars // 2)
        first_half = segment.iloc[:half]
        second_half = segment.iloc[half:]
        vol_first = safe_float(first_half["vol"].mean())
        vol_second = safe_float(second_half["vol"].mean())
        if vol_second > vol_first * 1.15:
            continue

        mean_rvol = safe_float(segment["rvol"].tail(min(48, bars)).mean())
        if mean_rvol > 1.15:
            continue

        pre_segment = df.iloc[max(0, len(df) - 1 - bars - 72):len(df) - 1 - bars].copy()
        pre_return = series_return(pre_segment["close"]) if not pre_segment.empty else 0.0
        contraction = safe_div(vol_first - vol_second, vol_first, 0.0)

        box_score = 100.0 * (
            0.25 * band_score(range_ratio, ACCUM_RANGE_MIN, ACCUM_RANGE_MAX, 0.03, 0.08) +
            0.20 * inverse_score(recent_adx, 14.0, ADX_FLAT_MAX) +
            0.20 * cap_score(contraction, 0.02, 0.30) +
            0.15 * band_score(mean_rvol, 0.35, 1.05, 0.45, 0.80) +
            0.10 * band_score(current_pos, 0.10, 1.10, 0.45, 0.95) +
            0.10 * cap_score(-pre_return, 0.02, 0.14)
        )

        candidate = {
            "high": seg_high,
            "low": seg_low,
            "bars": bars,
            "range_ratio": range_ratio,
            "current_pos": current_pos,
            "box_score": round(box_score, 2),
            "recent_adx": recent_adx,
            "segment": segment,
            "pre_segment": pre_segment,
            "pre_return": pre_return,
        }
        if best is None or candidate["box_score"] > best["box_score"]:
            best = candidate

    return best


def score_quiet_accumulation(df: pd.DataFrame, accum: dict) -> float:
    segment = accum["segment"]
    mean_rvol = safe_float(segment["rvol"].mean())
    price_drift = abs(series_return(segment["close"]))
    range_ratio = accum["range_ratio"]

    obv_slope = linear_slope(segment["obv"].tail(min(48, len(segment))))
    obv_norm = safe_div(obv_slope, max(safe_float(segment["vol"].mean()), 1.0), 0.0)

    cmf_values = segment["cmf"].dropna()
    cmf_start = safe_float(cmf_values.iloc[0]) if not cmf_values.empty else 0.0
    cmf_end = safe_float(cmf_values.iloc[-1]) if not cmf_values.empty else 0.0
    cmf_delta = cmf_end - cmf_start

    score = 100.0 * (
        0.30 * band_score(mean_rvol, 0.35, 1.00, 0.45, 0.70) +
        0.20 * band_score(range_ratio, ACCUM_RANGE_MIN, ACCUM_RANGE_MAX, 0.03, 0.08) +
        0.30 * (cap_score(obv_norm, 0.03, 0.25) * inverse_score(price_drift, 0.02, 0.10)) +
        0.20 * (
            0.5 * cap_score(cmf_end, -0.03, 0.08) +
            0.5 * cap_score(cmf_delta, 0.01, 0.10)
        )
    )
    return round(clamp(score, 0.0, 100.0), 2)


def score_volume_lead(df: pd.DataFrame, accum: dict) -> float:
    recent = df.iloc[-24:].copy()
    if recent.empty:
        return 0.0

    current_rvol = safe_float(df["rvol"].iloc[-1])
    recent_mean_rvol = safe_float(recent["rvol"].mean())
    base_mean_rvol = max(safe_float(accum["segment"]["rvol"].mean()), 0.1)
    price_runup = max(series_return(recent["close"]), 0.0)
    consecutive_volume = int((df["rvol"].tail(6) >= 1.05).sum())

    box_high = accum["high"]
    box_low = accum["low"]
    box_range = max(box_high - box_low, 1e-9)
    current_pos = safe_div(safe_float(df["close"].iloc[-1]) - box_low, box_range, 0.0)

    score = 100.0 * (
        0.30 * band_score(current_rvol, 1.05, 2.80, 1.20, 2.00) +
        0.25 * cap_score(safe_div(recent_mean_rvol, base_mean_rvol, 0.0), 1.15, 2.20) +
        0.20 * band_score(price_runup, 0.00, 0.12, 0.01, 0.08) +
        0.15 * cap_score(consecutive_volume, 2.0, 5.0) +
        0.10 * band_score(current_pos, 0.35, 1.12, 0.65, 1.02)
    )

    if current_rvol > 4.5:
        score -= 20.0
    if price_runup > 0.15:
        score -= 15.0

    return round(clamp(score, 0.0, 100.0), 2)


def score_obv_cmf(df: pd.DataFrame) -> Tuple[float, float, float]:
    recent = df.iloc[-24:].copy()
    if recent.empty:
        return 0.0, 0.0, 0.0

    obv_slope = linear_slope(recent["obv"])
    obv_norm = safe_div(obv_slope, max(safe_float(recent["vol"].mean()), 1.0), 0.0)
    cmf_now = safe_float(df["cmf"].iloc[-1])
    cmf_prev = safe_float(df["cmf"].iloc[-6]) if len(df) >= 6 else 0.0
    cmf_delta = cmf_now - cmf_prev

    score = 100.0 * (
        0.45 * cap_score(obv_norm, 0.03, 0.25) +
        0.35 * cap_score(cmf_now, -0.02, 0.10) +
        0.20 * cap_score(cmf_delta, 0.01, 0.08)
    )
    if obv_norm < 0 or cmf_now < -0.08:
        score *= 0.60

    return round(clamp(score, 0.0, 100.0), 2), obv_norm, cmf_now


def score_wyckoff_structure(df: pd.DataFrame, accum: dict) -> Tuple[float, dict]:
    segment = accum["segment"]
    box_high = accum["high"]
    box_low = accum["low"]
    box_range = max(box_high - box_low, 1e-9)
    bars = len(segment)
    third = max(6, bars // 3)

    pre_return = accum.get("pre_return", 0.0)
    prior_decline = 20.0 * cap_score(-pre_return, 0.03, 0.15)

    early = segment.iloc[:third]
    sc_idx = early["vol"].idxmax()
    sc_bar = segment.loc[sc_idx]
    sc_range = max(safe_float(sc_bar["high"]) - safe_float(sc_bar["low"]), 1e-9)
    sc_lower_wick = min(safe_float(sc_bar["open"]), safe_float(sc_bar["close"])) - safe_float(sc_bar["low"])
    sc_ratio = safe_div(sc_lower_wick, sc_range, 0.0)
    sc_vol_ratio = safe_div(safe_float(sc_bar["vol"]), max(safe_float(segment["vol"].mean()), 1.0), 0.0)
    sc_location = safe_div(safe_float(sc_bar["low"]) - box_low, box_range, 0.0)
    selling_climax = 20.0 * (
        cap_score(sc_vol_ratio, 1.30, 2.80) *
        cap_score(sc_ratio, 0.25, 0.55) *
        band_score(sc_location, -0.02, 0.25, -0.02, 0.10)
    )

    sc_pos = segment.index.get_loc(sc_idx)
    ar_slice = segment.iloc[sc_pos + 1:min(len(segment), sc_pos + 13)]
    rebound = 0.0
    if not ar_slice.empty:
        rebound = safe_div(safe_float(ar_slice["high"].max()) - safe_float(sc_bar["low"]), box_range, 0.0)
    automatic_rally = 15.0 * cap_score(rebound, 0.25, 0.75)

    st_slice = segment.iloc[third:]
    st_candidates = st_slice[st_slice["low"] <= box_low * 1.015]
    st_score = 0.0
    if not st_candidates.empty:
        st_vol_ratio = safe_div(safe_float(st_candidates["vol"].mean()), max(safe_float(sc_bar["vol"]), 1.0), 0.0)
        st_score = 15.0 * inverse_score(st_vol_ratio, 0.45, 0.95)

    recent = df.iloc[-24:].copy()
    spring_score = 0.0
    spring_low = safe_float(recent["low"].min()) if not recent.empty else box_low
    if not recent.empty:
        spring_idx = recent["low"].idxmin()
        spring_bar = recent.loc[spring_idx]
        pierced = spring_low < box_low and spring_low >= box_low * 0.97
        reclaimed = safe_float(spring_bar["close"]) > box_low
        wick_ratio = safe_div(
            min(safe_float(spring_bar["open"]), safe_float(spring_bar["close"])) - safe_float(spring_bar["low"]),
            max(safe_float(spring_bar["high"]) - safe_float(spring_bar["low"]), 1e-9),
            0.0,
        )
        if pierced and reclaimed:
            spring_score = 20.0 * (0.6 * cap_score(wick_ratio, 0.25, 0.55) + 0.4 * 1.0)

    recent12 = df.iloc[-12:].copy()
    higher_low = safe_float(recent12["low"].min()) > box_low * 1.003 if not recent12.empty else False
    current_pos = safe_div(safe_float(df["close"].iloc[-1]) - box_low, box_range, 0.0)
    recent_vol_ratio = safe_div(safe_float(recent12["vol"].mean()), max(safe_float(segment["vol"].mean()), 1.0), 0.0)
    lps_score = 20.0 * (
        0.45 * (1.0 if higher_low else 0.0) +
        0.35 * band_score(current_pos, 0.45, 1.05, 0.60, 0.95) +
        0.20 * inverse_score(recent_vol_ratio, 0.70, 1.10)
    )

    total = prior_decline + selling_climax + automatic_rally + st_score + spring_score + lps_score
    detail = {
        "prior_decline": round(prior_decline, 2),
        "selling_climax": round(selling_climax, 2),
        "automatic_rally": round(automatic_rally, 2),
        "secondary_test": round(st_score, 2),
        "spring": round(spring_score, 2),
        "lps": round(lps_score, 2),
    }
    return round(clamp(total, 0.0, 100.0), 2), detail


def is_breakout_candle(df: pd.DataFrame, accum_high: float) -> Tuple[bool, float, float]:
    last = df.iloc[-1]
    price = safe_float(last["close"])
    open_price = safe_float(last["open"])
    high = safe_float(last["high"])
    low = safe_float(last["low"])
    volume = safe_float(last["vol"])

    vol_ma20 = safe_float(df["vol"].iloc[-21:-1].mean()) if len(df) >= 21 else safe_float(df["vol"].iloc[:-1].mean())
    if vol_ma20 <= 0:
        return False, 0.0, 0.0

    vol_ratio = safe_div(volume, vol_ma20, 0.0)
    candle_range = high - low
    if candle_range <= 0:
        return False, vol_ratio, 0.0

    body_ratio = safe_div(abs(price - open_price), candle_range, 0.0)
    extension_pct = pct_change(accum_high, price) * 100.0 if price > accum_high else 0.0

    breakout = (
        price > accum_high and
        price > open_price and
        vol_ratio >= VOL_BREAKOUT_MULT and
        body_ratio >= BODY_RATIO_MIN
    )
    return breakout, vol_ratio, extension_pct


def score_liquidity(quote_volume: float) -> float:
    if quote_volume <= 0:
        return 0.0
    log_value = math.log10(quote_volume)
    return round(100.0 * cap_score(log_value, math.log10(MIN_QV_USD), 9.3), 2)


def compute_risk_penalty(
    price_change_pct: float,
    current_rvol: float,
    stop_pct: float,
    breakout_extension_pct: float,
    price: float,
    ema200: float,
) -> float:
    penalty = 0.0
    penalty += max(0.0, price_change_pct - 10.0) * 1.1
    penalty += max(0.0, current_rvol - 3.5) * 7.0
    penalty += max(0.0, stop_pct - 6.5) * 2.0
    penalty += max(0.0, breakout_extension_pct - 2.0) * 4.0

    if price_change_pct > MAX_24H_PUMP_PCT:
        penalty += 12.0
    if breakout_extension_pct > MAX_BREAKOUT_EXTENSION_PCT:
        penalty += 10.0
    if price < ema200 * 0.97:
        penalty += 8.0

    return round(min(penalty, 30.0), 2)


def classify_state(df: pd.DataFrame, accum: dict, volume_score: float, breakout: bool) -> Optional[str]:
    close = safe_float(df["close"].iloc[-1])
    box_high = accum["high"]
    box_low = accum["low"]
    box_range = max(box_high - box_low, 1e-9)
    current_pos = safe_div(close - box_low, box_range, 0.0)
    current_rvol = safe_float(df["rvol"].iloc[-1])
    recent_runup = max(series_return(df["close"].iloc[-24:]), 0.0)

    if breakout and volume_score >= 62.0 and current_rvol >= 1.6 and recent_runup <= 0.16:
        return "BREAKOUT"
    if current_pos >= 0.90 and volume_score >= 58.0 and current_rvol >= 1.15 and recent_runup <= 0.12:
        return "TRIGGER"
    if current_pos >= 0.55 and volume_score >= 40.0 and current_rvol <= 2.4:
        return "READY"
    return None


def build_trade_plan(df: pd.DataFrame, accum: dict, state: str) -> Optional[dict]:
    price = safe_float(df["close"].iloc[-1])
    atr = safe_float(df["atr"].iloc[-1])
    box_high = accum["high"]
    box_low = accum["low"]
    box_range = max(box_high - box_low, 1e-9)

    recent_low = safe_float(df["low"].iloc[-24:].min())
    stop_anchor = min(box_low, recent_low)
    stop_loss = stop_anchor - atr * 0.35
    if stop_loss >= price:
        return None

    stop_pct = safe_div(price - stop_loss, price, 0.0) * 100.0
    if stop_pct <= 0 or stop_pct > MAX_STOP_PCT:
        return None

    risk = price - stop_loss
    take_profit1 = price + risk * RR_RATIO
    take_profit2 = price + risk * RR_RATIO2
    be_target = price + risk * BE_TRIGGER
    box_mid = box_low + box_range * 0.55

    if state == "READY":
        zone_low = max(box_mid, price - atr * 0.40)
        zone_high = min(box_high * 0.995, price + atr * 0.35)
    elif state == "TRIGGER":
        zone_low = max(box_high * 0.985, price - atr * 0.30)
        zone_high = max(box_high * 1.005, price)
    else:
        zone_low = box_high
        zone_high = max(price, box_high + atr * 0.20)

    if zone_low > zone_high:
        zone_low, zone_high = zone_high, zone_low

    return {
        "entry": smart_round(price),
        "stop_loss": smart_round(stop_loss),
        "take_profit1": smart_round(take_profit1),
        "take_profit2": smart_round(take_profit2),
        "be_target": smart_round(be_target),
        "entry_zone": f"{fmt(smart_round(zone_low))} ~ {fmt(smart_round(zone_high))}",
        "invalidation": smart_round(stop_loss),
        "breakout_level": smart_round(box_high),
        "sl_pct_abs": round(stop_pct, 2),
        "tp1_pct": round(pct_change(price, take_profit1) * 100.0, 2),
        "tp2_pct": round(pct_change(price, take_profit2) * 100.0, 2),
    }


def build_comment(
    state: str,
    accumulation_score: float,
    volume_score: float,
    wyckoff_score: float,
    obv_cmf_score: float,
    current_rvol: float,
) -> str:
    reasons = []
    if accumulation_score >= 70:
        reasons.append("quiet accumulation box is stable")
    if volume_score >= 65:
        reasons.append(f"RVOL {current_rvol:.2f}x is leading price")
    if wyckoff_score >= 70:
        reasons.append("Spring/LPS structure quality is strong")
    if obv_cmf_score >= 60:
        reasons.append("OBV and CMF confirm money flow")
    if state == "TRIGGER":
        reasons.append("price is retesting the box ceiling")
    elif state == "BREAKOUT":
        reasons.append("volume-backed breakout is confirmed")
    else:
        reasons.append("late accumulation watch zone")
    return ", ".join(reasons[:3])


def analyze_symbol(meta: dict) -> Optional[dict]:
    symbol = meta["symbol"]
    try:
        raw = fetch_ohlcv(symbol, INTERVAL, KLINE_LIMIT)
        if len(raw) < max(320, ACCUM_BARS_MAX + 40):
            return None

        df = add_indicators(to_df(raw))
        if df.empty:
            return None

        price = safe_float(df["close"].iloc[-1])
        ema200 = safe_float(df["ema200"].iloc[-1])
        current_rvol = safe_float(df["rvol"].iloc[-1])

        accumulation = detect_accumulation(df)
        if accumulation is None:
            return None

        accumulation_score = score_quiet_accumulation(df, accumulation)
        volume_score = score_volume_lead(df, accumulation)
        obv_cmf_score, obv_norm, cmf_now = score_obv_cmf(df)
        wyckoff_score, wyckoff_detail = score_wyckoff_structure(df, accumulation)
        breakout, breakout_vol_ratio, breakout_extension_pct = is_breakout_candle(df, accumulation["high"])
        state = classify_state(df, accumulation, volume_score, breakout)
        if state is None:
            return None

        trade_plan = build_trade_plan(df, accumulation, state)
        if trade_plan is None:
            return None

        stop_pct_abs = trade_plan["sl_pct_abs"]
        liquidity_score = score_liquidity(meta["quote_volume"])
        risk_penalty = compute_risk_penalty(
            price_change_pct=meta["price_change_pct"],
            current_rvol=current_rvol,
            stop_pct=stop_pct_abs,
            breakout_extension_pct=breakout_extension_pct,
            price=price,
            ema200=ema200,
        )

        total_score = (
            0.30 * wyckoff_score +
            0.25 * accumulation_score +
            0.20 * volume_score +
            0.15 * obv_cmf_score +
            0.10 * liquidity_score
        )
        total_score -= risk_penalty
        if state == "TRIGGER":
            total_score += 3.0
        elif state == "BREAKOUT":
            total_score += 1.0

        total_score = round(clamp(total_score, 0.0, 100.0), 2)
        if total_score < MIN_RECOMMEND_SCORE:
            return None

        if state == "BREAKOUT" and breakout_extension_pct > MAX_BREAKOUT_EXTENSION_PCT:
            return None
        if meta["price_change_pct"] > MAX_24H_PUMP_PCT:
            return None
        if price < accumulation["low"] * 0.995:
            return None

        comment = build_comment(
            state=state,
            accumulation_score=accumulation_score,
            volume_score=volume_score,
            wyckoff_score=wyckoff_score,
            obv_cmf_score=obv_cmf_score,
            current_rvol=current_rvol,
        )

        return {
            "id": f"{symbol}_{int(time.time())}",
            "symbol": symbol_to_display(symbol),
            "raw_symbol": symbol,
            "timeframe": INTERVAL,
            "direction": "LONG",
            "state": state,
            "state_rank": STATE_PRIORITY[state],
            "total_score": total_score,
            "wyckoff_score": round(wyckoff_score, 2),
            "accumulation_score": round(accumulation_score, 2),
            "volume_score": round(volume_score, 2),
            "obv_cmf_score": round(obv_cmf_score, 2),
            "liquidity_score": round(liquidity_score, 2),
            "risk_penalty": round(risk_penalty, 2),
            "entry": trade_plan["entry"],
            "entry_zone": trade_plan["entry_zone"],
            "stop_loss": trade_plan["stop_loss"],
            "take_profit1": trade_plan["take_profit1"],
            "take_profit2": trade_plan["take_profit2"],
            "be_target": trade_plan["be_target"],
            "invalidation": trade_plan["invalidation"],
            "breakout_level": trade_plan["breakout_level"],
            "sl_pct": round(-stop_pct_abs, 2),
            "tp1_pct": trade_plan["tp1_pct"],
            "tp2_pct": trade_plan["tp2_pct"],
            "accum_high": smart_round(accumulation["high"]),
            "accum_low": smart_round(accumulation["low"]),
            "accum_bars": accumulation["bars"],
            "range_pct": round(accumulation["range_ratio"] * 100.0, 2),
            "rvol": round(current_rvol, 2),
            "vol_ratio": round(breakout_vol_ratio if breakout else current_rvol, 2),
            "adx": round(safe_float(df["adx"].iloc[-1]), 2),
            "cmf": round(cmf_now, 4),
            "obv_norm": round(obv_norm, 4),
            "quote_volume": meta["quote_volume"],
            "price_change_pct_24h": round(meta["price_change_pct"], 2),
            "comment": comment,
            "wyckoff_detail": wyckoff_detail,
            "status": "WATCH",
            "result_price": None,
            "result_pct": None,
            "result_time": None,
            "time": utc_now_str(),
        }
    except Exception:
        return None


def resolve_open_signals(signals: List[dict]) -> List[dict]:
    now = datetime.now(timezone.utc)
    open_symbols = list({
        signal.get("raw_symbol", signal.get("symbol", "").replace("/", ""))
        for signal in signals
        if signal.get("status") == "OPEN"
    })
    if not open_symbols:
        return []

    try:
        prices = fetch_last_prices(open_symbols)
    except Exception:
        return []

    resolved = []
    for signal in signals:
        if signal.get("status") != "OPEN":
            continue

        raw_symbol = signal.get("raw_symbol", signal.get("symbol", "").replace("/", ""))
        current = prices.get(raw_symbol)
        if current is None:
            continue

        try:
            signal_time = datetime.strptime(signal["time"], "%Y-%m-%d %H:%M UTC").replace(tzinfo=timezone.utc)
            elapsed_hours = (now - signal_time).total_seconds() / 3600.0
        except Exception:
            elapsed_hours = 0.0

        direction = signal.get("direction", "LONG")
        tp1 = signal.get("take_profit1")
        tp2 = signal.get("take_profit2", tp1)
        stop_loss = signal.get("stop_loss")
        result = None

        if direction == "LONG":
            if tp2 is not None and current >= tp2:
                result = "WIN"
            elif tp1 is not None and current >= tp1:
                result = "WIN"
            elif stop_loss is not None and current <= stop_loss:
                result = "LOSS"
            elif elapsed_hours >= EXPIRE_HOURS:
                result = "EXPIRED"
        else:
            if tp2 is not None and current <= tp2:
                result = "WIN"
            elif tp1 is not None and current <= tp1:
                result = "WIN"
            elif stop_loss is not None and current >= stop_loss:
                result = "LOSS"
            elif elapsed_hours >= EXPIRE_HOURS:
                result = "EXPIRED"

        if result is None:
            continue

        entry = signal.get("entry")
        if direction == "LONG":
            result_pct = round(pct_change(entry, current) * 100.0, 2) if entry else None
        else:
            result_pct = round((safe_div(entry - current, entry, 0.0) * 100.0), 2) if entry else None

        signal["status"] = result
        signal["result_price"] = smart_round(current)
        signal["result_pct"] = result_pct
        signal["result_time"] = utc_now_str()
        resolved.append(signal)

    return resolved


def summarize_recommendation(item: dict) -> str:
    return (
        f"{item['symbol']} | {item['state']} | score {item['total_score']:.1f} "
        f"| RVOL {item['rvol']:.2f}x | 24h {item['price_change_pct_24h']:+.2f}% "
        f"| box {fmt(item['accum_low'])}-{fmt(item['accum_high'])}"
    )


def main():
    print(f"Scanning Binance spot top {UNIVERSE_SIZE} liquid rising names on {INTERVAL}...")

    signals = load_signals()
    resolved = resolve_open_signals(signals)
    if resolved:
        print(f"Resolved open signals: {len(resolved)}")
        for signal in resolved:
            send_telegram(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, signal, is_result=True)

    open_keys = {
        (signal.get("raw_symbol", signal.get("symbol", "").replace("/", "")), signal.get("direction", "LONG"), signal.get("timeframe", INTERVAL))
        for signal in signals
        if signal.get("status") == "OPEN"
    }

    universe = get_top_symbols(UNIVERSE_SIZE, RISING_ONLY)
    recommendations = []

    for meta in universe:
        result = analyze_symbol(meta)
        time.sleep(0.08)
        if result is not None:
            recommendations.append(result)

    recommendations.sort(
        key=lambda item: (
            -item.get("total_score", 0.0),
            item.get("state_rank", 99),
            -item.get("quote_volume", 0.0),
        )
    )

    top_recommendations = recommendations[:TOP_RECOMMEND_N]
    save_recommendations(top_recommendations)

    print(f"Recommendations: {len(top_recommendations)} / Raw candidates: {len(recommendations)}")
    for item in top_recommendations:
        print(summarize_recommendation(item))

    new_actionable = []
    for item in top_recommendations:
        if item["state"] not in ACTIONABLE_STATES:
            continue
        if item["total_score"] < MIN_ACTIONABLE_SCORE:
            continue
        key = (item["raw_symbol"], item["direction"], item["timeframe"])
        if key in open_keys:
            continue
        actionable = dict(item)
        actionable["status"] = "OPEN"
        new_actionable.append(actionable)
        open_keys.add(key)

    telegram_items = new_actionable[:TOP_SIGNAL_N]
    print(f"Actionable signals: {len(new_actionable)} | Telegram send: {len(telegram_items)}")
    for item in telegram_items:
        send_telegram(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, item)

    if new_actionable:
        signals.extend(new_actionable)
        save_signals(signals)
    else:
        save_signals(signals)

    print(f"Saved recommendations -> {RECOMMENDATIONS_FILE}")
    print(f"Saved signals -> {SIGNALS_FILE}")
    print("Kronos scan complete.")


if __name__ == "__main__":
    main()
