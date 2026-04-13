import ccxt
import pandas as pd
import requests
import json
import os
from datetime import datetime, timezone

# ── 설정 ──────────────────────────────────────────
TELEGRAM_TOKEN   = os.environ['TELEGRAM_TOKEN']
TELEGRAM_CHAT_ID = os.environ['TELEGRAM_CHAT_ID']

EMA_PERIOD   = 200
ATR_PERIOD   = 14
RR_RATIO     = 3.0
BE_TRIGGER   = 1.5
EXPIRE_HOURS = 48

ENABLE_15M = True   # ← 15분 스캔 켜기/끄기 (False로 바꾸면 꺼짐)

SIGNALS_FILE = 'docs/signals.json'
# ──────────────────────────────────────────────────


def get_top100_symbols(ex):
    tickers = ex.fetch_tickers()
    usdt_pairs = {
        k: v for k, v in tickers.items()
        if k.endswith('/USDT:USDT') and k != 'BTC/USDT:USDT'
        and v.get('quoteVolume') is not None
    }
    sorted_pairs = sorted(usdt_pairs.items(),
                          key=lambda x: x[1]['quoteVolume'], reverse=True)
    return [s for s, _ in sorted_pairs[:100]]


def calculate_indicators(df):
    df = df.copy()
    df['s_high']   = df['high'].shift(1).rolling(20).max()
    df['s_low']    = df['low'].shift(1).rolling(20).min()
    df['bull_fvg'] = df['low'] > df['high'].shift(2)
    df['bear_fvg'] = df['high'] < df['low'].shift(2)
    df['ema200']   = df['close'].ewm(span=EMA_PERIOD, adjust=False).mean()

    hl  = df['high'] - df['low']
    hcp = abs(df['high'] - df['close'].shift(1))
    lcp = abs(df['low']  - df['close'].shift(1))
    tr  = pd.concat([hl, hcp, lcp], axis=1).max(axis=1)
    df['atr'] = tr.rolling(ATR_PERIOD).mean()
    return df


def scan_symbol(ex, symbol, timeframe='1h'):
    try:
        ohlcv = ex.fetch_ohlcv(symbol, timeframe, limit=250)
        if len(ohlcv) < 220:
            return None

        df = pd.DataFrame(ohlcv, columns=['ts','open','high','low','close','vol'])
        df = calculate_indicators(df)
        c  = df.iloc[-1]
        price, atr = c['close'], c['atr']

        if pd.isna(atr) or atr == 0:
            return None

        long_sig  = (price > c['s_high'] + atr * 0.1
                     and bool(c['bull_fvg'])
                     and price > c['ema200'])
        short_sig = (price < c['s_low']  - atr * 0.1
                     and bool(c['bear_fvg'])
                     and price < c['ema200'])

        if not (long_sig or short_sig):
            return None

        direction = 'LONG' if long_sig else 'SHORT'
        sl   = c['s_low']  if long_sig else c['s_high']
        dist = abs(price - sl)

        if dist < atr * 0.2:
            return None

        tp = price + dist * RR_RATIO if long_sig else price - dist * RR_RATIO
        be = price + dist * BE_TRIGGER if long_sig else price - dist * BE_TRIGGER

        sl_pct = round(abs(price - sl) / price * 100, 2)
        tp_pct = round(abs(tp - price) / price * 100, 2)

        return {
            'id':           f"{symbol.replace('/','_')}_{timeframe}_{int(datetime.now(timezone.utc).timestamp())}",
            'symbol':       symbol,
            'timeframe':    timeframe,
            'direction':    direction,
            'entry':        round(price, 6),
            'stop_loss':    round(sl,    6),
            'take_profit':  round(tp,    6),
            'be_target':    round(be,    6),
            'sl_pct':       sl_pct,
            'tp_pct':       tp_pct,
            'atr':          round(atr,   6),
            'rr':           RR_RATIO,
            'status':       'OPEN',
            'result_price': None,
            'result_pct':   None,
            'result_time':  None,
            'time':         datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC'),
        }

    except Exception as e:
        print(f"  {symbol} [{timeframe}] 스캔 실패: {e}")
        return None


def resolve_open_signals(ex, signals):
    now = datetime.now(timezone.utc)
    open_symbols = list({s['symbol'] for s in signals if s.get('status') == 'OPEN'})
    if not open_symbols:
        return []

    prices = {}
    try:
        tickers = ex.fetch_tickers(open_symbols)
        for sym, t in tickers.items():
            prices[sym] = float(t['last'])
    except Exception as e:
        print(f"  현재가 조회 실패: {e}")
        return []

    resolved = []
    for sig in signals:
        if sig.get('status') != 'OPEN':
            continue

        curr = prices.get(sig['symbol'])
        if curr is None:
            continue

        try:
            sig_time = datetime.strptime(sig['time'], '%Y-%m-%d %H:%M UTC').replace(tzinfo=timezone.utc)
            elapsed  = (now - sig_time).total_seconds() / 3600
        except Exception:
            elapsed = 0

        result = None
        if sig['direction'] == 'LONG':
            if   curr >= sig['take_profit']: result = 'WIN'
            elif curr <= sig['stop_loss']:   result = 'LOSS'
            elif elapsed >= EXPIRE_HOURS:    result = 'EXPIRED'
        else:
            if   curr <= sig['take_profit']: result = 'WIN'
            elif curr >= sig['stop_loss']:   result = 'LOSS'
            elif elapsed >= EXPIRE_HOURS:    result = 'EXPIRED'

        if result:
            entry = sig['entry']
            if sig['direction'] == 'LONG':
                result_pct = round((curr - entry) / entry * 100, 2)
            else:
                result_pct = round((entry - curr) / entry * 100, 2)

            sig['status']       = result
            sig['result_price'] = round(curr, 6)
            sig['result_pct']   = result_pct
            sig['result_time']  = now.strftime('%Y-%m-%d %H:%M UTC')
            resolved.append(sig)

    return resolved


def send_telegram(token, chat_id, signal, is_result=False):
    tf = signal.get('timeframe', '1h')
    if is_result:
        icon    = {'WIN': '🏆', 'LOSS': '💀', 'EXPIRED': '⏰'}.get(signal['status'], '❓')
        pct     = signal.get('result_pct')
        pct_str = f"\n손익: `{'+' if pct and pct >= 0 else ''}{pct}%`" if pct is not None else ''
        msg = (f"{icon} *결과 확정: {signal['symbol']}* [{tf}]\n"
               f"방향: {signal['direction']}\n"
               f"결과: *{signal['status']}*\n"
               f"진입가: `{signal['entry']}`\n"
               f"종료가: `{signal['result_price']}`"
               f"{pct_str}\n"
               f"{signal['result_time']}")
    else:
        icon = '🚀' if signal['direction'] == 'LONG' else '🎯'
        msg  = (f"{icon} *{signal['symbol']}* [{tf}] "
                f"{'🟢 LONG' if signal['direction'] == 'LONG' else '🔴 SHORT'}\n"
                f"진입: `{signal['entry']}`\n"
                f"손절: `{signal['stop_loss']}` (-{signal['sl_pct']}%)\n"
                f"목표: `{signal['take_profit']}` (+{signal['tp_pct']}%)\n"
                f"본절: `{signal['be_target']}`\n"
                f"RR: 1:{signal['rr']}\n"
                f"{signal['time']}")
    try:
        requests.get(
            f"https://api.telegram.org/bot{token}/sendMessage",
            params={'chat_id': chat_id, 'text': msg, 'parse_mode': 'Markdown'},
            timeout=5
        )
    except Exception as e:
        print(f"  텔레그램 전송 실패: {e}")


def load_signals():
    if os.path.exists(SIGNALS_FILE):
        with open(SIGNALS_FILE, 'r') as f:
            return json.load(f)
    return []


def save_signals(signals):
    os.makedirs(os.path.dirname(SIGNALS_FILE), exist_ok=True)
    with open(SIGNALS_FILE, 'w') as f:
        json.dump(signals[-1000:], f, ensure_ascii=False, indent=2)


def main():
    ex = ccxt.bybit({'options': {'defaultType': 'linear'}})

    # 1. OPEN 시그널 결과 판정
    signals  = load_signals()
    resolved = resolve_open_signals(ex, signals)
    if resolved:
        print(f"  결과 확정: {len(resolved)}건")
        for sig in resolved:
            send_telegram(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, sig, is_result=True)
            print(f"  {sig['symbol']} → {sig['status']} @ {sig['result_price']} ({sig.get('result_pct','?')}%)")

    # 2. 이미 OPEN 중인 symbol+direction 목록 (중복 방지)
    open_set = {(s['symbol'], s['direction']) for s in signals if s.get('status') == 'OPEN'}

    # 3. 종목 목록 1회만 조회
    print("시총 상위 100 종목 스캔 중...")
    symbols = get_top100_symbols(ex)

    new_signals = []

    # 4. 1h 스캔
    print("[1h] 스캔 시작...")
    for sym in symbols:
        result = scan_symbol(ex, sym, '1h')
        if result and (result['symbol'], result['direction']) not in open_set:
            new_signals.append(result)
            open_set.add((result['symbol'], result['direction']))
            print(f"  ✅ [1h] {result['symbol']} {result['direction']}")

    # 5. 15m 스캔 (옵션)
    if ENABLE_15M:
        print("[15m] 스캔 시작...")
        for sym in symbols:
            result = scan_symbol(ex, sym, '15m')
            if result and (result['symbol'], result['direction']) not in open_set:
                new_signals.append(result)
                open_set.add((result['symbol'], result['direction']))
                print(f"  ✅ [15m] {result['symbol']} {result['direction']}")

    print(f"  새 시그널 {len(new_signals)}건")
    for sig in new_signals:
        send_telegram(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, sig)

    # 6. 저장
    signals.extend(new_signals)
    save_signals(signals)
    print("완료")


if __name__ == '__main__':
    main()
