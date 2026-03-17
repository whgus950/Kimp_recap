"""
collector.py — 김치프리미엄 데이터 수집 모듈 (v2)

모든 4가지 거래소 조합의 김프를 동시에 계산합니다.
빗썸/업비트 × 바이비트/바이낸스 = 4가지 조합
"""

import requests
import json
import time as _time

# ─── API Endpoints ───────────────────────────────────────────────

API = {
    'bithumb': {
        'ticker_all': 'https://api.bithumb.com/public/ticker/ALL_KRW',
        'usdt': 'https://api.bithumb.com/public/ticker/USDT_KRW',
        'asset_status': 'https://api.bithumb.com/public/assetsstatus/ALL',
        'network_info': 'https://api.bithumb.com/v2/fee/inout/',
    },
    'upbit': {
        'markets': 'https://api.upbit.com/v1/market/all?isDetails=false',
        'ticker': 'https://api.upbit.com/v1/ticker',
    },
    'bybit': {
        'tickers': 'https://api.bytick.com/v5/market/tickers?category=linear',
    },
    'binance': {
        'tickers': 'https://fapi.binance.com/fapi/v1/ticker/price',
    },
    'exchange_rate': 'https://api.exchangerate-api.com/v4/latest/USD',
}

COMBOS = [
    ('bithumb', 'bybit'),
    ('bithumb', 'binance'),
    ('upbit', 'bybit'),
    ('upbit', 'binance'),
]


# ─── Helpers ─────────────────────────────────────────────────────

def fetch_json(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'
    }
    resp = requests.get(url, headers=headers, timeout=15, verify=False)
    resp.raise_for_status()
    return resp.json()


# ─── Bithumb ─────────────────────────────────────────────────────

def fetch_bithumb_tickers():
    data = fetch_json(API['bithumb']['ticker_all'])
    if data.get('status') != '0000':
        raise Exception(f"Bithumb ticker error: {data.get('message')}")
    tickers = {}
    for symbol, info in data['data'].items():
        if symbol == 'date':
            continue
        try:
            tickers[symbol] = {
                'price': float(info['closing_price']),
                'volume24h': float(info.get('units_traded_24H', 0)),
            }
        except (ValueError, KeyError):
            continue
    return tickers


def fetch_bithumb_usdt_price():
    data = fetch_json(API['bithumb']['usdt'])
    if data.get('status') != '0000':
        raise Exception(f"Bithumb USDT error: {data.get('message')}")
    return float(data['data']['closing_price'])


def fetch_bithumb_asset_status():
    data = fetch_json(API['bithumb']['asset_status'])
    if data.get('status') != '0000':
        raise Exception(f"Bithumb asset status error: {data.get('message')}")
    result = {}
    for symbol, info in data['data'].items():
        result[symbol] = {
            'deposit_enabled': info.get('deposit_status') == 1,
            'withdrawal_enabled': info.get('withdrawal_status') == 1,
        }
    return result


def fetch_bithumb_network_info(symbol):
    url = API['bithumb']['network_info'] + symbol
    try:
        data = fetch_json(url)
        networks = []
        if isinstance(data, list):
            for item in data:
                for net in item.get('networks', []):
                    n = net.get('net_name', '')
                    if n and n not in networks:
                        networks.append(n)
        elif isinstance(data, dict) and 'data' in data:
            info = data['data']
            if isinstance(info, list):
                for item in info:
                    for net in item.get('networks', []):
                        n = net.get('net_name', '')
                        if n and n not in networks:
                            networks.append(n)
        return networks
    except Exception:
        return []


# ─── Upbit ───────────────────────────────────────────────────────

def fetch_upbit_markets():
    data = fetch_json(API['upbit']['markets'])
    return [m['market'] for m in data if m['market'].startswith('KRW-')]


def fetch_upbit_tickers():
    markets = fetch_upbit_markets()
    tickers = {}
    chunk_size = 100
    for i in range(0, len(markets), chunk_size):
        chunk = markets[i:i + chunk_size]
        url = f"{API['upbit']['ticker']}?markets={','.join(chunk)}"
        data = fetch_json(url)
        for item in data:
            symbol = item['market'].replace('KRW-', '')
            tickers[symbol] = {
                'price': float(item['trade_price']),
                'volume24h': float(item.get('acc_trade_volume_24h', 0)),
            }
    return tickers


# ─── Bybit ───────────────────────────────────────────────────────

def fetch_bybit_tickers():
    data = fetch_json(API['bybit']['tickers'])
    if data.get('retCode') != 0:
        raise Exception(f"Bybit error: {data.get('retMsg')}")
    tickers = {}
    for item in data['result']['list']:
        if not item['symbol'].endswith('USDT'):
            continue
        symbol = item['symbol'].replace('USDT', '')
        tickers[symbol] = {
            'price': float(item['lastPrice']),
            'volume24h': float(item.get('volume24h', 0)),
        }
    return tickers


# ─── Binance ─────────────────────────────────────────────────────

def fetch_binance_tickers():
    data = fetch_json(API['binance']['tickers'])
    tickers = {}
    for item in data:
        if not item['symbol'].endswith('USDT'):
            continue
        symbol = item['symbol'].replace('USDT', '')
        tickers[symbol] = {
            'price': float(item['price']),
        }
    return tickers


# ─── Exchange Rate ───────────────────────────────────────────────

def fetch_usd_krw_rate():
    data = fetch_json(API['exchange_rate'])
    return data['rates']['KRW']


# ─── Premium Calculation ─────────────────────────────────────────

def calculate_premiums(domestic_tickers, foreign_tickers, usdt_krw_price, usd_krw_rate):
    tether_premium = round(((usdt_krw_price / usd_krw_rate) - 1) * 100, 2)
    premiums = {}
    for symbol, domestic in domestic_tickers.items():
        if symbol == 'USDT':
            continue
        if symbol not in foreign_tickers:
            continue
        krw_price = domestic['price']
        usdt_price = foreign_tickers[symbol]['price']
        fair_krw_price = usdt_price * usdt_krw_price
        if fair_krw_price == 0:
            continue
        premium = round(((krw_price / fair_krw_price) - 1) * 100, 2)
        premiums[symbol] = {
            'symbol': symbol,
            'premium': premium,
            'krwPrice': krw_price,
            'usdtPrice': usdt_price,
        }
    return premiums, tether_premium


def filter_by_tether_premium(premiums, tether_premium, threshold=5):
    min_premium = tether_premium + threshold
    return {s: d for s, d in premiums.items() if d['premium'] >= min_premium}


# ─── All-Combo Snapshot ──────────────────────────────────────────

def collect_all_combos():
    """
    4가지 거래소 조합의 김프를 한번에 수집합니다.
    각 조합에 대해 필터링된 코인과 테더김프를 반환합니다.
    """
    from datetime import datetime, timezone, timedelta
    KST = timezone(timedelta(hours=9))

    # 1. 모든 시세를 한번에 가져오기
    print("    API 호출: ", end="", flush=True)
    bithumb_tickers = fetch_bithumb_tickers()
    print("빗썸 ", end="", flush=True)

    upbit_tickers = fetch_upbit_tickers()
    print("업비트 ", end="", flush=True)

    bybit_tickers = fetch_bybit_tickers()
    print("바이비트 ", end="", flush=True)

    binance_tickers = fetch_binance_tickers()
    print("바이낸스 ", end="", flush=True)

    usd_krw_rate = fetch_usd_krw_rate()
    print("환율 ", end="", flush=True)

    # 2. USDT/KRW 가격
    bithumb_usdt = fetch_bithumb_usdt_price()
    upbit_usdt = upbit_tickers.get('USDT', {}).get('price', usd_krw_rate)

    # 3. 빗썸 입출금 상태
    asset_status = {}
    try:
        asset_status = fetch_bithumb_asset_status()
        print("입출금 ", end="", flush=True)
    except Exception as e:
        print(f"(입출금실패) ", end="", flush=True)

    print("완료")

    domestic_map = {
        'bithumb': (bithumb_tickers, bithumb_usdt),
        'upbit': (upbit_tickers, upbit_usdt),
    }
    foreign_map = {
        'bybit': bybit_tickers,
        'binance': binance_tickers,
    }

    # 4. 4가지 조합 계산
    combos = {}
    timestamp = datetime.now(KST).strftime('%H:%M:%S')

    for dom_name, (dom_tickers, dom_usdt) in domestic_map.items():
        for for_name, for_tickers in foreign_map.items():
            combo_key = f"{dom_name}-{for_name}"
            premiums, tether_prem = calculate_premiums(
                dom_tickers, for_tickers, dom_usdt, usd_krw_rate
            )
            filtered = filter_by_tether_premium(premiums, tether_prem, 5)

            combos[combo_key] = {
                'tetherPremium': tether_prem,
                'premiums': filtered,
                'timestamp': timestamp,
            }

    return combos, asset_status


# ─── Network Info (for filtered coins) ───────────────────────────

def enrich_with_network_info(all_symbols, asset_status):
    """필터 통과한 코인들의 네트워크 정보를 조회합니다."""
    for symbol in all_symbols:
        if symbol in asset_status:
            nets = fetch_bithumb_network_info(symbol)
            asset_status[symbol]['networks'] = nets
        else:
            nets = fetch_bithumb_network_info(symbol)
            asset_status[symbol] = {'networks': nets}
        _time.sleep(0.12)  # Rate limit
    return asset_status


# ─── Test mode ───────────────────────────────────────────────────

def run_test():
    print('=== 전체 거래소 조합 김프 수집 테스트 ===\n')
    try:
        combos, asset_status = collect_all_combos()
        for combo_key, data in combos.items():
            coins = list(data['premiums'].values())
            coins.sort(key=lambda c: c['premium'], reverse=True)
            print(f"\n  [{combo_key}] 테더김프: {data['tetherPremium']}% | 필터통과: {len(coins)}개")
            for c in coins[:5]:
                print(f"    {c['symbol']:<8} {c['premium']:>7.2f}%")
            if len(coins) > 5:
                print(f"    ... 외 {len(coins)-5}개")
    except Exception as e:
        print(f"  ERROR: {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    import sys
    if '--test' in sys.argv:
        run_test()
