"""
backfill.py — 과거 김치프리미엄 데이터 복원

거래소 캔들스틱(1분봉) API를 사용하여 과거 특정 시간대의
김프를 재구성합니다.

사용법:
  python backfill.py                          # 오늘 0시 + 어제 9시
  python backfill.py --date=2026-03-05 --hour=9   # 특정 날짜/시간
"""

import os
import sys
import json
import time as _time
from datetime import datetime, timezone, timedelta
from collector import (
    fetch_json, fetch_bithumb_asset_status, fetch_bithumb_network_info,
    enrich_with_network_info,
    API, COMBOS,
)
import urllib.request

KST = timezone(timedelta(hours=9))
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
os.makedirs(DATA_DIR, exist_ok=True)


# ─── Candlestick APIs ────────────────────────────────────────

def fetch_bithumb_candles(symbol, start_ts, end_ts):
    """
    빗썸 1분봉 데이터를 가져옵니다.
    https://api.bithumb.com/public/candlestick/{currency}_KRW/1m
    응답: [[timestamp, open, close, high, low, volume], ...]
    """
    url = f"https://api.bithumb.com/public/candlestick/{symbol}_KRW/1m"
    try:
        data = fetch_json(url)
        if data.get('status') != '0000':
            return []
        candles = data.get('data', [])
        # 시간 범위 필터링 (timestamp는 밀리초)
        filtered = []
        for c in candles:
            ts = int(c[0])
            if start_ts <= ts <= end_ts:
                filtered.append({
                    'timestamp': ts,
                    'close': float(c[2]),  # close price
                })
        return filtered
    except Exception:
        return []


def fetch_bybit_candles(symbol, start_ts, end_ts):
    """
    바이비트 1분봉 데이터를 가져옵니다.
    GET /v5/market/kline?category=linear&symbol={symbol}USDT&interval=1&start={ms}&end={ms}
    """
    url = (f"https://api.bybit.com/v5/market/kline"
           f"?category=linear&symbol={symbol}USDT&interval=1"
           f"&start={start_ts}&end={end_ts}&limit=200")
    try:
        data = fetch_json(url)
        if data.get('retCode') != 0:
            return []
        candles = []
        for item in data['result']['list']:
            candles.append({
                'timestamp': int(item[0]),
                'close': float(item[4]),  # close price
            })
        return sorted(candles, key=lambda x: x['timestamp'])
    except Exception:
        return []


def fetch_binance_candles(symbol, start_ts, end_ts):
    """바이낸스 선물 1분봉 데이터."""
    url = (f"https://fapi.binance.com/fapi/v1/klines"
           f"?symbol={symbol}USDT&interval=1m"
           f"&startTime={start_ts}&endTime={end_ts}&limit=200")
    try:
        data = fetch_json(url)
        candles = []
        for item in data:
            candles.append({
                'timestamp': int(item[0]),
                'close': float(item[4]),
            })
        return candles
    except Exception:
        return []


def fetch_upbit_candles(symbol, target_dt, count=60):
    """
    업비트 1분봉 데이터.
    GET /v1/candles/minutes/1?market=KRW-{symbol}&to={ISO}&count=16
    """
    # 업비트의 to 파라미터는 UTC ISO 8601 형식
    to_utc = target_dt.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')
    url = (f"https://api.upbit.com/v1/candles/minutes/1"
           f"?market=KRW-{symbol}&to={to_utc}&count={count}")
    try:
        data = fetch_json(url)
        candles = []
        for item in data:
            ts = datetime.fromisoformat(
                item['candle_date_time_utc'].replace('T', ' ')
            ).replace(tzinfo=timezone.utc)
            candles.append({
                'timestamp': int(ts.timestamp() * 1000),
                'close': float(item['trade_price']),
            })
        return sorted(candles, key=lambda x: x['timestamp'])
    except Exception:
        return []


# ─── Get all symbols from exchanges ──────────────────────────

def get_bithumb_symbols():
    """빗썸에 상장된 전체 KRW 마켓 심볼 목록."""
    data = fetch_json(API['bithumb']['ticker_all'])
    if data.get('status') != '0000':
        return []
    return [s for s in data['data'].keys() if s != 'date']


def get_upbit_symbols():
    """업비트 KRW 마켓 심볼 목록."""
    data = fetch_json(API['upbit']['markets'])
    return [m['market'].replace('KRW-', '') for m in data if m['market'].startswith('KRW-')]


def get_bybit_symbols():
    """바이비트 USDT-PERP 심볼 목록."""
    data = fetch_json(API['bybit']['tickers'])
    if data.get('retCode') != 0:
        return []
    return [item['symbol'].replace('USDT', '')
            for item in data['result']['list']
            if item['symbol'].endswith('USDT')]


def get_binance_symbols():
    """바이낸스 USDT 선물 심볼 목록."""
    data = fetch_json(API['binance']['tickers'])
    return [item['symbol'].replace('USDT', '')
            for item in data if item['symbol'].endswith('USDT')]


# ─── Backfill Logic ──────────────────────────────────────────

def backfill_window(target_date_str, start_hour, start_min=0, duration_min=16):
    """
    특정 시간 윈도우의 김프를 캔들스틱 데이터로 복원합니다.

    Args:
        target_date_str: "2026-03-06" 형태
        start_hour: 시작 시간 (KST)
        start_min: 시작 분
        duration_min: 수집 시간(분)
    """
    # 시간 범위 계산
    target_start = datetime.strptime(target_date_str, '%Y-%m-%d').replace(
        hour=start_hour, minute=start_min, second=0,
        tzinfo=KST
    )
    target_end = target_start + timedelta(minutes=duration_min)
    start_ms = int(target_start.timestamp() * 1000)
    end_ms = int(target_end.timestamp() * 1000)

    print(f"\n{'='*60}")
    print(f"  Backfill: {target_date_str} {start_hour:02d}:{start_min:02d}~"
          f"{start_hour:02d}:{start_min+duration_min:02d} KST")
    print(f"{'='*60}")

    # 1. 거래소별 심볼 목록
    print("\n  Fetching symbol lists...")
    bithumb_syms = set(get_bithumb_symbols())
    upbit_syms = set(get_upbit_symbols())
    bybit_syms = set(get_bybit_symbols())
    binance_syms = set(get_binance_symbols())
    print(f"    빗썸: {len(bithumb_syms)} | 업비트: {len(upbit_syms)} | "
          f"바이비트: {len(bybit_syms)} | 바이낸스: {len(binance_syms)}")

    # 2. USDT 캔들 데이터 (테더김프용)
    print("\n  Fetching USDT candles...")
    bithumb_usdt_candles = fetch_bithumb_candles('USDT', start_ms, end_ms)

    # 실제 환율 (최근값 사용 - 하루 내 변동 미미)
    from collector import fetch_usd_krw_rate
    usd_krw = fetch_usd_krw_rate()
    print(f"    USDT candles: {len(bithumb_usdt_candles)} | USD/KRW: {usd_krw}")

    # 3. 공통 심볼에 대해 캔들 데이터 수집
    # 4가지 조합의 공통 심볼
    combo_symbols = {
        'bithumb-bybit': bithumb_syms & bybit_syms,
        'bithumb-binance': bithumb_syms & binance_syms,
        'upbit-bybit': upbit_syms & bybit_syms,
        'upbit-binance': upbit_syms & binance_syms,
    }

    # 모든 조합에서 필요한 고유 심볼
    all_needed = set()
    for syms in combo_symbols.values():
        all_needed.update(syms)
    all_needed.discard('USDT')

    print(f"\n  Total unique symbols to check: {len(all_needed)}")

    # 4. 각 거래소별 캔들 데이터 수집 (캐싱)
    candle_cache = {
        'bithumb': {},
        'upbit': {},
        'bybit': {},
        'binance': {},
    }

    # 빗썸 캔들 수집
    bithumb_needed = (bithumb_syms & all_needed) - {'USDT'}
    print(f"\n  Fetching Bithumb candles ({len(bithumb_needed)} symbols)...", end="", flush=True)
    count = 0
    for sym in bithumb_needed:
        candles = fetch_bithumb_candles(sym, start_ms, end_ms)
        if candles:
            candle_cache['bithumb'][sym] = candles
        count += 1
        if count % 50 == 0:
            print(f" {count}", end="", flush=True)
        _time.sleep(0.05)  # Rate limit
    print(f" done ({len(candle_cache['bithumb'])} with data)")

    # 업비트 캔들 수집
    upbit_needed = (upbit_syms & all_needed) - {'USDT'}
    print(f"  Fetching Upbit candles ({len(upbit_needed)} symbols)...", end="", flush=True)
    count = 0
    for sym in upbit_needed:
        candles = fetch_upbit_candles(sym, target_end, count=60)
        if candles:
            # 이전 캔들을 활용하기 위해 start_ms 하한선 필터 제거
            candles = [c for c in candles if c['timestamp'] <= end_ms]
            if candles:
                candle_cache['upbit'][sym] = candles
        count += 1
        if count % 50 == 0:
            print(f" {count}", end="", flush=True)
        _time.sleep(0.1)  # 업비트 rate limit 더 엄격
    print(f" done ({len(candle_cache['upbit'])} with data)")

    # 바이비트 캔들 수집
    bybit_needed = (bybit_syms & all_needed) - {'USDT'}
    print(f"  Fetching Bybit candles ({len(bybit_needed)} symbols)...", end="", flush=True)
    count = 0
    for sym in bybit_needed:
        candles = fetch_bybit_candles(sym, start_ms, end_ms)
        if candles:
            candle_cache['bybit'][sym] = candles
        count += 1
        if count % 50 == 0:
            print(f" {count}", end="", flush=True)
        _time.sleep(0.05)
    print(f" done ({len(candle_cache['bybit'])} with data)")

    # 바이낸스 캔들 수집
    binance_needed = (binance_syms & all_needed) - {'USDT'}
    print(f"  Fetching Binance candles ({len(binance_needed)} symbols)...", end="", flush=True)
    count = 0
    for sym in binance_needed:
        candles = fetch_binance_candles(sym, start_ms, end_ms)
        if candles:
            candle_cache['binance'][sym] = candles
        count += 1
        if count % 50 == 0:
            print(f" {count}", end="", flush=True)
        _time.sleep(0.05)
    print(f" done ({len(candle_cache['binance'])} with data)")

    # 5. 입출금 상태
    print("\n  Fetching asset status...")
    try:
        asset_status = fetch_bithumb_asset_status()
    except Exception:
        asset_status = {}

    # 6. 각 조합별 김프 계산
    print("\n  Computing premiums for each combo...")
    combinations = {}

    if start_hour == 0:
        valid_combos = ['bithumb-bybit', 'bithumb-binance']
    elif start_hour == 9:
        valid_combos = ['upbit-bybit', 'upbit-binance']
    else:
        valid_combos = ['bithumb-bybit', 'bithumb-binance', 'upbit-bybit', 'upbit-binance']

    for combo_key in valid_combos:
        dom_name, for_name = combo_key.split('-')
        common = combo_symbols[combo_key] - {'USDT'}

        # 각 1분 타임스탬프에서의 김프 계산
        # 타임스탬프 목록 생성 (1분 간격)
        minute_stamps = []
        t = start_ms
        while t <= end_ms:
            minute_stamps.append(t)
            t += 60000

        # USDT/KRW 가격 (시간별)
        usdt_prices = {}
        if dom_name == 'bithumb':
            for c in bithumb_usdt_candles:
                usdt_prices[c['timestamp']] = c['close']
        # 업비트 USDT
        if dom_name == 'upbit' and 'USDT' in candle_cache.get('upbit', {}):
            for c in candle_cache['upbit']['USDT']:
                usdt_prices[c['timestamp']] = c['close']

        # 기본 USDT 가격 (캔들이 없을 때)
        default_usdt = (
            bithumb_usdt_candles[0]['close'] if bithumb_usdt_candles
            else usd_krw
        )

        coin_map = {}

        for sym in common:
            dom_candles = candle_cache.get(dom_name, {}).get(sym, [])
            for_candles = candle_cache.get(for_name, {}).get(sym, [])

            if not dom_candles or not for_candles:
                continue

            # 타임스탬프별 인덱스 생성
            dom_by_ts = {c['timestamp']: c['close'] for c in dom_candles}
            for_by_ts = {c['timestamp']: c['close'] for c in for_candles}

            snapshots = []
            
            for ts in sorted(set(dom_by_ts.keys()) & set(for_by_ts.keys())):
                krw_price = dom_by_ts[ts]
                usdt_price = for_by_ts[ts]
                
                usdt_krw = _find_nearest(usdt_prices, ts, default_usdt)
                tether_prem = round(((usdt_krw / usd_krw) - 1) * 100, 2)

                fair_krw = usdt_price * usdt_krw
                if fair_krw == 0:
                    continue
                premium = round(((krw_price / fair_krw) - 1) * 100, 2)

                # 테더김프 + 5% 필터
                if premium >= tether_prem + 5:
                    ts_dt = datetime.fromtimestamp(ts / 1000, tz=KST)
                    snapshots.append({
                        'time': ts_dt.strftime('%H:%M:%S'),
                        'premium': premium,
                        'krwPrice': krw_price,
                        'usdtPrice': usdt_price,
                    })

            if snapshots:
                prems = [s['premium'] for s in snapshots]
                coin_map[sym] = {
                    'symbol': sym,
                    'avgPremium': round(sum(prems) / len(prems), 2),
                    'maxPremium': round(max(prems), 2),
                    'depositEnabled': asset_status.get(sym, {}).get('deposit_enabled', False),
                    'withdrawalEnabled': asset_status.get(sym, {}).get('withdrawal_enabled', False),
                    'networks': [],
                    'snapshots': snapshots,
                    'appearances': len(snapshots),
                }

        coins = sorted(coin_map.values(), key=lambda x: x['avgPremium'], reverse=True)

        # 테더 김프 계산
        tether_list = []
        for ts, usdt_p in usdt_prices.items():
            tether_list.append(round(((usdt_p / usd_krw) - 1) * 100, 2))
        if not tether_list:
            tether_list = [round(((default_usdt / usd_krw) - 1) * 100, 2)]

        combinations[combo_key] = {
            'tetherPremium': {
                'avg': round(sum(tether_list) / len(tether_list), 2),
                'max': round(max(tether_list), 2),
            },
            'totalSnapshots': max(len(s.get('snapshots', [])) for s in coins) if coins else 0,
            'coins': coins,
        }

        print(f"    {combo_key}: {len(coins)} coins")

    # 7. 네트워크 정보
    all_filtered = set()
    for combo in combinations.values():
        for coin in combo['coins']:
            all_filtered.add(coin['symbol'])

    if all_filtered:
        print(f"\n  Enriching network info ({len(all_filtered)} coins)...")
        asset_status = enrich_with_network_info(all_filtered, asset_status)
        for combo in combinations.values():
            for coin in combo['coins']:
                coin['networks'] = asset_status.get(coin['symbol'], {}).get('networks', [])

    # 8. 저장
    is_weekend = datetime.strptime(target_date_str, '%Y-%m-%d').weekday() >= 5
    result = {
        'date': target_date_str,
        'isWeekend': is_weekend,
        'backfilled': True,
        'window': f"{start_hour:02d}:{start_min:02d}-{start_hour:02d}:{start_min+duration_min:02d}",
        'combinations': combinations,
    }

    file_path = os.path.join(DATA_DIR, f'{target_date_str}.json')

    # 기존 파일이 있으면 병합 (다른 시간대 데이터 보존)
    if os.path.exists(file_path):
        with open(file_path, 'r', encoding='utf-8') as f:
            existing = json.load(f)
        # 기존 combinations에 새 데이터 덮어쓰기
        if 'combinations' in existing:
            existing['combinations'].update(combinations)
            result = existing
            result['combinations'] = existing['combinations']

    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    total = sum(len(c['coins']) for c in combinations.values())
    print(f"\n  Saved: {file_path}")
    print(f"  {total} total coin entries across 4 combos")
    return result


def _find_nearest(price_map, target_ts, default):
    """가장 가까운 타임스탬프의 가격을 찾습니다."""
    if not price_map:
        return default
    if target_ts in price_map:
        return price_map[target_ts]
    closest = min(price_map.keys(), key=lambda t: abs(t - target_ts))
    return price_map[closest]


def auto_backfill():
    """Bithumb의 제공 범위 한도 내에서 가능한 모든 과거 날짜를 자동으로 복원합니다."""
    print(">>> Determining maximum backfill range from Bithumb API...")
    url = "https://api.bithumb.com/public/candlestick/BTC_KRW/1m"
    try:
        bithumb_data = fetch_json(url)
        candles = bithumb_data.get('data', [])
        earliest_ms = int(candles[0][0])
        earliest_kst = datetime.fromtimestamp(earliest_ms / 1000, tz=KST)
        print(f"    Earliest Bithumb candle available: {earliest_kst.strftime('%Y-%m-%d %H:%M:%S')} KST")
    except Exception as e:
        print(f"Error checking Bithumb API: {e}")
        return

    now_kst = datetime.now(KST)
    available_dates = []

    # 최근 5일까지 검사
    for i in range(5):
        target_date = (now_kst - timedelta(days=i))
        date_str = target_date.strftime('%Y-%m-%d')
        
        # 00:00 빗썸 시간대가 가능범위 안에 있는지 확인
        bithumb_window_start = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
        upbit_window_start = target_date.replace(hour=9, minute=0, second=0, microsecond=0)

        tasks = []
        if bithumb_window_start >= earliest_kst:
            if bithumb_window_start <= now_kst:
                tasks.append((date_str, 0))
        if upbit_window_start >= earliest_kst:
            if upbit_window_start <= now_kst:
                tasks.append((date_str, 9))

        for date_s, hr in tasks:
            available_dates.append((date_s, hr))
    
    # 시간 순으로 오름차순 정렬 (가장 옛날 것부터)
    available_dates.sort(key=lambda x: (x[0], x[1]))

    print(f"    Found {len(available_dates)} time windows to backfill.")
    for d, h in available_dates:
        print(f"      - {d} {h:02d}:00")

    for d, h in available_dates:
        backfill_window(d, h)

# ─── Entry Point ─────────────────────────────────────────────

if __name__ == '__main__':
    args = sys.argv[1:]
    now_kst = datetime.now(KST)

    if '--date' in ''.join(args):
        # 특정 날짜/시간
        date_str = None
        hour = 0
        for a in args:
            if a.startswith('--date='):
                date_str = a.split('=')[1]
            if a.startswith('--hour='):
                hour = int(a.split('=')[1])
        if date_str:
            backfill_window(date_str, hour)
    else:
        # 기본: 가능한 모든 범위 자동 백필
        auto_backfill()
