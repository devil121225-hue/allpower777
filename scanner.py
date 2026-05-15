#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Kronos 알트코인 스캐너 v3.0
전략: Wyckoff 매집 구간 감지 + 거래량 돌파 진입
"""

import json
import math
import os
import time
from datetime import datetime, timezone

import pandas as pd
import requests

# ─────────────────────────────────────────────────────────────
# 환경 변수
# ─────────────────────────────────────────────────────────────
TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN",   "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# ─────────────────────────────────────────────────────────────
# 전략 파라미터
# ─────────────────────────────────────────────────────────────
EMA_PERIOD   = 200
ATR_PERIOD   = 14
RR_RATIO     = 2.0    # TP1 RR 1:2
RR_RATIO2    = 3.0    # TP2 RR 1:3
BE_TRIGGER   = 1.5
EXPIRE_HOURS = 48

TOP_N        = 100
TOP_SIGNAL_N = 10

# ── Wyckoff 매집 구간 파라미터 ──────────────────────────────
ACCUM_BARS_MIN   = 15    # 매집 구간 최소 봉 수
ACCUM_BARS_MAX   = 50    # 매집 구간 최대 봉 수
ACCUM_RANGE_MAX  = 0.07  # 매집 구간 최대 가격 범위 비율 (7%)
ACCUM_RANGE_MIN  = 0.01  # 매집 구간 최소 가격 범위 비율 (1% — 너무 좁으면 제외)
ADX_FLAT_MAX     = 28    # ADX 이 값 이하일 때 횡보로 판단
VOL_BREAKOUT_MULT = 1.5  # 돌파 캔들 거래량 배수 (20봉 평균 대비)
BODY_RATIO_MIN   = 0.45  # 돌파 캔들 몸통 비율 최소값 (캔들 전체 대비)

SIGNALS_FILE = "docs/signals.json"
BASE_URL = "https://api1.binance.com"

STABLE_BASES = {
    "USDT", "USDC", "FDUSD", "TUSD", "BUSD",
    "DAI",  "USDP", "USDE",  "UST",  "USTC",
    "AEUR", "XUSD",
}


# ─────────────────────────────────────────────────────────────
# 유틸
# ─────────────────────────────────────────────────────────────
def utc_now_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def smart_round(v):
    """가격 크기에 따라 소수점 자릿수 자동 조정 (PEPE 같은 소액 코인 대응)"""
    if v is None or v == 0:
        return v
    digits = max(6, -int(math.floor(math.log10(abs(v)))) + 4)
    return round(v, digits)


def fmt(v) -> str:
    if v is None:
        return "-"
    s = f"{float(v):.10f}".rstrip("0").rstrip(".")
    return s


def load_signals():
    if not os.path.exists(SIGNALS_FILE):
        return []
    try:
        with open(SIGNALS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []


def save_signals(signals):
    os.makedirs("docs", exist_ok=True)
    tmp = SIGNALS_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(signals[-1000:], f, ensure_ascii=False, indent=2)
    os.replace(tmp, SIGNALS_FILE)


# ─────────────────────────────────────────────────────────────
# 텔레그램
# ─────────────────────────────────────────────────────────────
def send_telegram(token, chat_id, signal, is_result: bool = False):
    if not token or not chat_id:
        return

    symbol    = signal["symbol"]
    direction = signal["direction"]
    entry     = fmt(signal["entry"])
    sl        = fmt(signal["stop_loss"])
    tp1       = fmt(signal.get("take_profit1", signal.get("take_profit")))
    tp2       = fmt(signal.get("take_profit2", signal.get("take_profit")))
    be        = fmt(signal.get("be_target"))
    slp       = signal.get("sl_pct")
    tp1p      = signal.get("tp1_pct")
    tp2p      = signal.get("tp2_pct")
    ts        = signal.get("time", "")
    qv        = signal.get("quote_volume", 0)

    qv_str   = f"{qv / 1e6:.1f}M" if qv >= 1e6 else f"{qv / 1e3:.0f}K"
    dir_icon = "🟢" if direction == "LONG" else "🔴"

    if is_result:
        status  = signal["status"]
        icons   = {"WIN": "🏆", "LOSS": "💀", "EXPIRED": "⏰"}
        icon    = icons.get(status, "❓")
        rp      = fmt(signal.get("result_price"))
        pct     = signal.get("result_pct")
        pct_str = f"{'+'if pct and pct>=0 else ''}{pct:.2f}%" if pct is not None else "?"
        msg = (
            f"{icon} 결과: {symbol}\n"
            f"방향: {direction} | 결과: {status}\n"
            f"진입: {entry} → {rp}\n"
            f"손익: {pct_str}\n"
            f"{ts}"
        )
    else:
        def pct_str(v):
            if v is None: return "?"
            return f"{'+' if v >= 0 else ''}{v:.2f}%"

        # 매집 구간 정보 추가
        accum_info = ""
        if signal.get("accum_high") and signal.get("accum_low"):
            accum_info = (
                f"매집구간: {fmt(signal['accum_low'])} ~ {fmt(signal['accum_high'])}\n"
                f"구간봉수: {signal.get('accum_bars', '?')}봉 | "
                f"돌파거래량: {signal.get('vol_ratio', 0):.1f}x\n"
            )

        msg = (
            "알트종목 시그널 공유:\n"
            f"🚀 {symbol} {dir_icon} {direction}\n"
            f"진입: {entry}\n"
            f"손절: {sl} ({pct_str(slp)})\n"
            f"TP1: {tp1} ({pct_str(tp1p)}) RR 1:{int(RR_RATIO)}\n"
            f"TP2: {tp2} ({pct_str(tp2p)}) RR 1:{int(RR_RATIO2)}\n"
            f"본절: {be}\n"
            f"{accum_info}"
            f"거래대금: {qv_str}\n"
            f"{ts}"
        )

    try:
        requests.get(
            f"https://api.telegram.org/bot{token}/sendMessage",
            params={"chat_id": chat_id, "text": msg},
            timeout=10,
        )
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────
# 데이터 / 지표
# ─────────────────────────────────────────────────────────────
def symbol_to_display(symbol):
    return symbol[:-4] + "/USDT" if symbol.endswith("USDT") else symbol


def fetch_ohlcv(symbol, interval="1h", limit=300):
    r = requests.get(
        f"{BASE_URL}/api/v3/klines",
        params={"symbol": symbol, "interval": interval, "limit": limit},
        timeout=20,
    )
    r.raise_for_status()
    return r.json()


def to_df(raw):
    return pd.DataFrame(
        [[x[0], float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5])]
         for x in raw],
        columns=["ts", "open", "high", "low", "close", "vol"],
    )


def calc_ema(df, period):
    return df["close"].ewm(span=period, adjust=False).mean()


def calc_atr(df, period=ATR_PERIOD):
    prev_close = df["close"].shift(1)
    tr = pd.concat([
        df["high"] - df["low"],
        (df["high"] - prev_close).abs(),
        (df["low"]  - prev_close).abs(),
    ], axis=1).max(axis=1)
    return float(tr.iloc[-period:].mean())


def calc_adx(df, period=14):
    """ADX 계산 (횡보 판단용)"""
    h, l, c = df["high"], df["low"], df["close"]
    prev_h  = h.shift(1)
    prev_l  = l.shift(1)
    prev_c  = c.shift(1)

    dm_plus  = ((h - prev_h).clip(lower=0)
                .where((h - prev_h) > (prev_l - l), 0))
    dm_minus = ((prev_l - l).clip(lower=0)
                .where((prev_l - l) > (h - prev_h), 0))

    tr = pd.concat([
        h - l,
        (h - prev_c).abs(),
        (l - prev_c).abs(),
    ], axis=1).max(axis=1)

    atr_s    = tr.ewm(span=period, adjust=False).mean()
    di_plus  = 100 * dm_plus.ewm(span=period, adjust=False).mean() / atr_s
    di_minus = 100 * dm_minus.ewm(span=period, adjust=False).mean() / atr_s
    dx       = (100 * (di_plus - di_minus).abs()
                / (di_plus + di_minus).replace(0, float("nan")))
    adx      = dx.ewm(span=period, adjust=False).mean()
    return float(adx.iloc[-1]) if not adx.empty else 50.0


def get_top_symbols(top_n=TOP_N):
    MIN_QV_USD = 20_000_000
    info    = requests.get(f"{BASE_URL}/api/v3/exchangeInfo", timeout=20).json()
    tickers = requests.get(f"{BASE_URL}/api/v3/ticker/24hr",  timeout=20).json()

    allowed = set()
    for s in info.get("symbols", []):
        if not isinstance(s, dict): continue
        if s.get("status") != "TRADING": continue
        if s.get("quoteAsset") != "USDT": continue
        if s.get("isSpotTradingAllowed") is not True: continue
        if s.get("baseAsset", "") in STABLE_BASES: continue
        allowed.add(s["symbol"])

    rows = []
    for t in tickers:
        if not isinstance(t, dict): continue
        sym = t.get("symbol")
        if sym not in allowed: continue
        try:
            qv = float(t.get("quoteVolume", 0))
        except Exception:
            continue
        if qv >= MIN_QV_USD:
            rows.append((sym, qv))

    rows.sort(key=lambda x: x[1], reverse=True)
    return {s: qv for s, qv in rows[:top_n]}


def fetch_last_prices(symbols):
    r = requests.get(f"{BASE_URL}/api/v3/ticker/price", timeout=20)
    r.raise_for_status()
    want = set(symbols)
    return {
        row["symbol"]: float(row["price"])
        for row in r.json()
        if isinstance(row, dict) and row.get("symbol") in want
    }


# ─────────────────────────────────────────────────────────────
# Wyckoff 매집 구간 감지
# ─────────────────────────────────────────────────────────────
def detect_accumulation(df: pd.DataFrame, direction: str):
    """
    최근 봉들에서 Wyckoff 매집 구간 탐색.

    조건:
    1. ACCUM_BARS_MIN ~ ACCUM_BARS_MAX 구간의 가격 범위가
       평균가 대비 ACCUM_RANGE_MIN ~ ACCUM_RANGE_MAX 이내
    2. ADX < ADX_FLAT_MAX (추세 없는 횡보)
    3. 구간 내 거래량이 직전 동일 기간 대비 감소 (매집 특징)
    4. 방향 검증:
       - LONG:  현재가가 매집 구간 고점 근처 또는 위 (고점이 현재가 아래여야 함)
       - SHORT: 현재가가 매집 구간 저점 근처 또는 아래 (저점이 현재가 위여야 함)

    반환: (accum_high, accum_low, accum_bars) or None
    """
    if len(df) < ACCUM_BARS_MAX + 20:
        return None

    current_price = float(df["close"].iloc[-1])

    # 현재 봉 제외 (돌파 판단은 scan_symbol에서)
    body = df.iloc[-(ACCUM_BARS_MAX + 1):-1].copy()

    best = None  # (range_ratio, accum_high, accum_low, bars)

    for n in range(ACCUM_BARS_MIN, ACCUM_BARS_MAX + 1):
        segment  = body.iloc[-n:]
        seg_high = float(segment["high"].max())
        seg_low  = float(segment["low"].min())
        avg_price = float(segment["close"].mean())

        if avg_price == 0:
            continue

        range_ratio = (seg_high - seg_low) / avg_price

        # 가격 범위 조건
        if not (ACCUM_RANGE_MIN <= range_ratio <= ACCUM_RANGE_MAX):
            continue

        # ── 방향 검증 ─────────────────────────────────────────
        # LONG:  매집 구간이 현재가 아래 또는 현재가 = 구간 고점 부근
        #        (현재가가 구간 고점을 막 돌파한 상태)
        # SHORT: 매집 구간이 현재가 위 또는 현재가 = 구간 저점 부근
        if direction == "LONG":
            # 구간 고점이 현재가보다 너무 높으면 제외
            # (현재가가 구간 고점의 130% 이상이면 이미 한참 올라간 것)
            if seg_high > current_price * 1.30:
                continue
            # 구간 전체가 현재가보다 훨씬 위면 제외
            if seg_low > current_price * 1.05:
                continue
        else:  # SHORT
            # 구간 저점이 현재가보다 너무 낮으면 제외
            if seg_low < current_price * 0.70:
                continue
            # 구간 전체가 현재가보다 훨씬 아래면 제외
            if seg_high < current_price * 0.95:
                continue

        # ADX 횡보 조건
        adx_val = calc_adx(df.iloc[-(n + 20):], period=14)
        if adx_val >= ADX_FLAT_MAX:
            continue

        # 거래량 감소 조건
        prev_segment = body.iloc[-(n * 2):-n] if len(body) >= n * 2 else None
        if prev_segment is not None and len(prev_segment) >= n // 2:
            vol_now  = float(segment["vol"].mean())
            vol_prev = float(prev_segment["vol"].mean())
            vol_declining = vol_now <= vol_prev * 1.1
        else:
            vol_declining = True

        if not vol_declining:
            continue

        # 가장 좁은(압축된) 범위 선택
        if best is None or range_ratio < best[0]:
            best = (range_ratio, seg_high, seg_low, n)

    if best is None:
        return None

    _, accum_high, accum_low, accum_bars = best
    return accum_high, accum_low, accum_bars


# ─────────────────────────────────────────────────────────────
# 돌파 캔들 검증
# ─────────────────────────────────────────────────────────────
def is_breakout_candle(df: pd.DataFrame, direction: str, accum_high: float, accum_low: float) -> tuple:
    """
    마지막 봉이 매집 구간을 강한 거래량으로 돌파하는지 검증.

    반환: (True/False, vol_ratio)
    """
    last     = df.iloc[-1]
    price    = float(last["close"])
    open_    = float(last["open"])
    high_    = float(last["high"])
    low_     = float(last["low"])
    vol      = float(last["vol"])

    # 20봉 평균 거래량
    vol_ma20 = float(df["vol"].iloc[-21:-1].mean())
    if vol_ma20 == 0:
        return False, 0.0

    vol_ratio = vol / vol_ma20

    # 거래량 돌파 조건
    if vol_ratio < VOL_BREAKOUT_MULT:
        return False, vol_ratio

    # 캔들 몸통 비율 조건 (강한 돌파 = 긴 몸통)
    candle_range = high_ - low_
    if candle_range == 0:
        return False, vol_ratio

    body_size  = abs(price - open_)
    body_ratio = body_size / candle_range

    if body_ratio < BODY_RATIO_MIN:
        return False, vol_ratio

    # 방향별 돌파 확인
    if direction == "LONG":
        # 종가가 매집 구간 고점 위
        if price <= accum_high:
            return False, vol_ratio
        # 불리시 캔들 (종가 > 시가)
        if price <= open_:
            return False, vol_ratio
    else:
        # 종가가 매집 구간 저점 아래
        if price >= accum_low:
            return False, vol_ratio
        # 베어리시 캔들 (종가 < 시가)
        if price >= open_:
            return False, vol_ratio

    return True, vol_ratio


# ─────────────────────────────────────────────────────────────
# 심볼 스캔 (Wyckoff 전략)
# ─────────────────────────────────────────────────────────────
def scan_symbol(symbol, quote_volume=0):
    """
    1h 데이터 기준 Wyckoff 매집 + 거래량 돌파 전략:

    진입 조건:
    1. 매집 구간 감지 (ADX < 28, 가격 범위 1~7%, 거래량 감소)
    2. 현재 봉이 매집 구간 고점(LONG) / 저점(SHORT) 돌파
    3. 돌파 캔들 거래량 > 20봉 평균 × 1.5
    4. 돌파 캔들 몸통 비율 > 45%
    5. EMA200 방향 확인 (LONG: 가격 > EMA200)

    SL/TP:
    - LONG  SL: 매집 구간 저점 - ATR × 0.5
    - SHORT SL: 매집 구간 고점 + ATR × 0.5
    - TP1: 진입 ± dist × 2  (RR 1:2)
    - TP2: 진입 ± dist × 3  (RR 1:3)
    - BE:  진입 ± dist × 1.5
    """
    try:
        raw = fetch_ohlcv(symbol, "1h", 300)
        if len(raw) < 250:
            return None

        df     = to_df(raw)
        ema200 = calc_ema(df, EMA_PERIOD)
        atr    = calc_atr(df, ATR_PERIOD)
        price  = float(df["close"].iloc[-1])
        ema200_val = float(ema200.iloc[-1])

        # ── 1. EMA200 방향 판단 ──────────────────────────────
        direction = "LONG" if price > ema200_val else "SHORT"

        # ── 2. 매집 구간 감지 ───────────────────────────────
        accum = detect_accumulation(df, direction)
        if accum is None:
            return None

        accum_high, accum_low, accum_bars = accum

        # ── 3. 돌파 캔들 검증 ───────────────────────────────
        breakout, vol_ratio = is_breakout_candle(df, direction, accum_high, accum_low)
        if not breakout:
            return None

        # ── 4. SL / TP 계산 ─────────────────────────────────
        if direction == "LONG":
            sl   = accum_low - atr * 0.5
            dist = price - sl
            if dist <= 0:
                return None
            # SL이 진입가보다 위에 있으면 잘못된 것
            if sl >= price:
                return None
            tp1 = price + dist * RR_RATIO
            tp2 = price + dist * RR_RATIO2
            be  = price + dist * BE_TRIGGER
            # TP가 음수거나 진입가보다 낮으면 제외
            if tp1 <= price or tp2 <= price:
                return None
        else:
            sl   = accum_high + atr * 0.5
            dist = sl - price
            if dist <= 0:
                return None
            # SL이 진입가보다 아래에 있으면 잘못된 것
            if sl <= price:
                return None
            tp1 = price - dist * RR_RATIO
            tp2 = price - dist * RR_RATIO2
            be  = price - dist * BE_TRIGGER
            # TP가 음수거나 진입가보다 높으면 제외
            if tp1 >= price or tp2 >= price or tp1 <= 0 or tp2 <= 0:
                return None

        # SL 비율이 너무 크면 (10% 초과) 스킵
        sl_pct_abs = abs(price - sl) / price * 100
        if sl_pct_abs > 10:
            return None

        sl_pct  = round(-sl_pct_abs, 2)
        tp1_pct = round(abs(tp1 - price) / price * 100, 2)
        tp2_pct = round(abs(tp2 - price) / price * 100, 2)

        return {
            "id":           f"{symbol}_{int(time.time())}",
            "symbol":       symbol_to_display(symbol),
            "raw_symbol":   symbol,
            "timeframe":    "1h",
            "direction":    direction,
            "entry":        smart_round(price),
            "stop_loss":    smart_round(sl),
            "take_profit1": smart_round(tp1),
            "take_profit2": smart_round(tp2),
            "be_target":    smart_round(be),
            "sl_pct":       sl_pct,
            "tp1_pct":      tp1_pct,
            "tp2_pct":      tp2_pct,
            "accum_high":   smart_round(accum_high),
            "accum_low":    smart_round(accum_low),
            "accum_bars":   accum_bars,
            "vol_ratio":    round(vol_ratio, 2),
            "adx":          round(calc_adx(df), 1),
            "quote_volume": quote_volume,
            "status":       "OPEN",
            "result_price": None,
            "result_pct":   None,
            "result_time":  None,
            "time":         utc_now_str(),
        }

    except Exception:
        return None


# ─────────────────────────────────────────────────────────────
# 시그널 결과 갱신
# ─────────────────────────────────────────────────────────────
def resolve_open_signals(signals):
    """OPEN 시그널 TP/SL/EXPIRED 자동 판정"""
    now = datetime.now(timezone.utc)

    open_symbols = list({
        s.get("raw_symbol", s["symbol"].replace("/", ""))
        for s in signals if s.get("status") == "OPEN"
    })
    if not open_symbols:
        return []

    try:
        prices = fetch_last_prices(open_symbols)
    except Exception:
        return []

    resolved = []
    for sig in signals:
        if sig.get("status") != "OPEN":
            continue

        raw_symbol = sig.get("raw_symbol", sig["symbol"].replace("/", ""))
        curr = prices.get(raw_symbol)
        if curr is None:
            continue

        try:
            sig_time = datetime.strptime(
                sig["time"], "%Y-%m-%d %H:%M UTC"
            ).replace(tzinfo=timezone.utc)
            elapsed = (now - sig_time).total_seconds() / 3600
        except Exception:
            elapsed = 0

        tp1 = sig.get("take_profit1", sig.get("take_profit"))
        tp2 = sig.get("take_profit2", tp1)

        result = None
        if sig["direction"] == "LONG":
            if curr >= tp2:   result = "WIN"
            elif curr >= tp1: result = "WIN"
            elif curr <= sig["stop_loss"]:    result = "LOSS"
            elif elapsed >= EXPIRE_HOURS:     result = "EXPIRED"
        else:
            if curr <= tp2:   result = "WIN"
            elif curr <= tp1: result = "WIN"
            elif curr >= sig["stop_loss"]:    result = "LOSS"
            elif elapsed >= EXPIRE_HOURS:     result = "EXPIRED"

        if result:
            entry = sig["entry"]
            if sig["direction"] == "LONG":
                result_pct = round((curr - entry) / entry * 100, 2)
            else:
                result_pct = round((entry - curr) / entry * 100, 2)

            sig["status"]       = result
            sig["result_price"] = smart_round(curr)
            sig["result_pct"]   = result_pct
            sig["result_time"]  = utc_now_str()
            resolved.append(sig)

    return resolved


# ─────────────────────────────────────────────────────────────
# 메인
# ─────────────────────────────────────────────────────────────
def main():
    print(f"거래대금 상위 {TOP_N}개 바이낸스 현물 알트코인 스캔 중...")

    signals = load_signals()

    # 미결 시그널 결과 확정
    resolved = resolve_open_signals(signals)
    if resolved:
        print(f"기존 시그널 결과 확정: {len(resolved)}건")
        for sig in resolved:
            send_telegram(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, sig, is_result=True)

    # 이미 OPEN 중인 조합 수집
    open_set = {
        (s.get("raw_symbol", s["symbol"].replace("/", "")), s["direction"], s["timeframe"])
        for s in signals if s.get("status") == "OPEN"
    }

    symbol_vol = get_top_symbols(TOP_N)
    symbols    = list(symbol_vol.keys())

    new_signals = []
    for sym in symbols:
        result = scan_symbol(sym, symbol_vol.get(sym, 0))
        time.sleep(0.1)

        if result is None:
            continue

        key = (result["raw_symbol"], result["direction"], result["timeframe"])
        if key in open_set:
            continue

        new_signals.append(result)
        open_set.add(key)

    new_signals.sort(key=lambda x: x.get("quote_volume", 0), reverse=True)
    telegram_signals = new_signals[:TOP_SIGNAL_N]

    print(f"새 시그널 전체 {len(new_signals)}건")
    print(f"텔레그램 전송 대상 상위 {len(telegram_signals)}건")

    for sig in telegram_signals:
        print(f"[전송] {sig['symbol']} {sig['direction']} "
              f"| 매집{sig['accum_bars']}봉 | 거래량{sig['vol_ratio']}x | ADX{sig['adx']}")
        send_telegram(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, sig)

    for sig in new_signals[TOP_SIGNAL_N:]:
        print(f"[저장만] {sig['symbol']} {sig['direction']}")

    signals.extend(new_signals)
    save_signals(signals)
    print("Kronos 스캔 완료")


if __name__ == "__main__":
    main()
