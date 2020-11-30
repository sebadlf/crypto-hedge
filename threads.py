import threading
import pandas as pd
import datetime as dt

from okex_utils import dato_actual_ponderado as dato_okex
from binance import dato_actual as dato_binance
from bitfinex import dato_actual as dato_bitfinex
from hitbtc import dato_actual as dato_hitbtc
from config import TICKERS

start = dt.datetime.now()

BROKERS = [{
    'name': 'okex',
    'dato_actual': dato_okex
},
    {
    'name': 'binance',
    'dato_actual': dato_binance
}, {
    'name': 'bitfinex',
    'dato_actual': dato_bitfinex
}, {
    'name': 'hitbtc',
    'dato_actual': dato_hitbtc
}]

data = []

def worker(ticker, broker):
    dato_actual = broker['dato_actual']

    try:
        value = dato_actual(ticker, 'USDT')

        data.append({
            'broker': broker['name'],
            'ticker': ticker,
            'ask': float(value[0]),
            'bid': float(value[1]),
        })

    except:
        print(f'Error en {broker["name"]} - {ticker}')

threads = []
for ticker in TICKERS:

    for broker in BROKERS:

        t = threading.Thread(target=worker, args=(ticker, broker))

        threads.append(t)
        t.start()

for t in threads:
    t.join()

print(dt.datetime.now() - start)

print(pd.DataFrame(data).sort_values("ticker"))

# sp500_earnings = pd.concat(dfs)
# sp500_earnings