import ccxt
import pandas as pd
import requests
import json
import os
from datetime import datetime, timezone

TELEGRAM_TOKEN   = os.environ['TELEGRAM_TOKEN']
TELEGRAM_CHAT_ID = os.environ['TELEGRAM_CHAT_ID']

EMA_PERIOD   = 200
ATR_PERIOD   = 14
RR_RATIO     = 3.0
BE_TRIGGER   = 1.5
EXPIRE_HOURS = 48

ENABLE_15M = True

SIGNALS_FILE = 'docs/signals.json'


def get_top100_symbols(ex):
    tickers = ex.fetch_tickers()
    usdt_pairs = {}
    for k, v in tickers.items():
        if k.endswith('/USDT-SWAP') and k != 'BTC/USDT-SWAP':
            if v.get('quoteVolume') is not None:
                usdt_pairs[k] = v
    sorted_pairs = sorted(usdt_pairs.items(), key=lambda x: x[1]['quoteVolume'], reverse=True)
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
        price = c['close']
        atr   = c['atr']
        if pd.isna(atr) or atr == 0:
            return None
        long_sig  = (price > c['s_high'] + atr * 0.1 and bool(c['bull_fvg']) and price > c['ema200'])
        short_sig = (price < c['s_low']  - atr * 0.1 and bool(c['bear_fvg']) and price < c['ema200'])
        if not (long_sig or short_sig):
            return None
        direction = 'LONG' if long_sig else 'SHORT'
        sl   = c['s_low'] if long_sig else c['s_high']
        dist = abs(price - sl)
        if dist < atr * 0.2:
            return None
        tp = price + dist * RR_RATIO if long_sig else price - dist * RR_RATIO
        be = price + dist * BE_TRIGGER if long_sig else price - dist * BE_TRIGGER
        sl_pct = round(abs(price - sl) / price * 100, 2)
        tp_pct = round(abs(tp - price) / price * 100, 2)
        sig_id = symbol.replace('/', '_') + '_' + timeframe + '_' + str(int(datetime.now(timezone.utc).timestamp()))
        return {
            'id':           sig_id,
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
        print("  " + symbol + " [" + timeframe + "] 스캔 실패: " + str(e))
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
        print("  현재가 조회 실패: " + str(e))
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
    tf     = signal.get('timeframe', '1h')
    sym    = signal['symbol']
    direct = signal['direction']
    entry  = str(signal['entry'])
    sl     = str(signal['stop_loss'])
    tp     = str(signal['take_profit'])
    be     = str(signal['be_target'])
    rr     = str(signal['rr'])
    slp    = str(signal['sl_pct'])
    tpp    = str(signal['tp_pct'])
    ts     = signal['time']

    if is_result:
        status = signal['status']
        icons  = {'WIN': '🏆', 'LOSS': '💀', 'EXPIRED': '⏰'}
        icon   = icons.get(status, '❓')
        pct    = signal.get('result_pct')
        rp     = str(signal['result_price'])
        rt     = str(signal['result_time'])
        if pct is not None:
            sign    = '+' if pct >= 0 else ''
            pct_str = '\n손익: `' + sign + str(pct) + '%`'
        else:
            pct_str = ''
        msg = (icon + ' *결과 확정: ' + sym + '* [' + tf + ']\n'
               + '방향: ' + direct + '\n'
               + '결과: *' + status + '*\n'
               + '진입가: `' + entry + '`\n'
               + '종료가: `' + rp + '`'
               + pct_str + '\n'
               + rt)
    else:
        if direct == 'LONG':
            icon   = '🚀'
            dirlbl = '🟢 LONG'
        else:
            icon   = '🎯'
            dirlbl = '🔴 SHORT'
        msg = (icon + ' *' + sym + '* [' + tf + '] ' + dirlbl + '\n'
               + '진입: `' + entry + '`\n'
               + '손절: `' + sl + '` (-' + slp + '%)\n'
               + '목표: `' + tp + '` (+' + tpp + '%)\n'
               + '본절: `' + be + '`\n'
               + 'RR: 1:' + rr + '\n'
               + ts)
    try:
        requests.get(
            'https://api.telegram.org/bot' + token + '/sendMessage',
            params={'chat_id': chat_id, 'text': msg, 'parse_mode': 'Markdown'},
            timeout=5
        )
    except Exception as e:
        print("  텔레그램 전송 실패: " + str(e))


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
    ex = ccxt.okx({'options': {'defaultType': 'swap'}})

    signals  = load_signals()
    resolved = resolve_open_signals(ex, signals)
    if resolved:
        print("  결과 확정: " + str(len(resolved)) + "건")
        for sig in resolved:
            send_telegram(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, sig, is_result=True)
            print("  " + sig['symbol'] + " -> " + sig['status'])

    open_set = {(s['symbol'], s['direction']) for s in signals if s.get('status') == 'OPEN'}

    print("시총 상위 100 종목 스캔 중...")
    symbols = get_top100_symbols(ex)

    new_signals = []

    print("[1h] 스캔 시작...")
    for sym in symbols:
        result = scan_symbol(ex, sym, '1h')
        if result and (result['symbol'], result['direction']) not in open_set:
            new_signals.append(result)
            open_set.add((result['symbol'], result['direction']))
            print("  [1h] " + result['symbol'] + " " + result['direction'])

    if ENABLE_15M:
        print("[15m] 스캔 시작...")
        for sym in symbols:
            result = scan_symbol(ex, sym, '15m')
            if result and (result['symbol'], result['direction']) not in open_set:
                new_signals.append(result)
                open_set.add((result['symbol'], result['direction']))
                print("  [15m] " + result['symbol'] + " " + result['direction'])

    print("  새 시그널 " + str(len(new_signals)) + "건")
    for sig in new_signals:
        send_telegram(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, sig)

    signals.extend(new_signals)
    save_signals(signals)
    print("완료")


if __name__ == '__main__':
    main()
