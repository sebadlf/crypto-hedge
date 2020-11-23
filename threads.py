import threading
import pandas as pd

from okex_utils import dato_actual as dato_okex
from binance import dato_actual as dato_binance
from bitfinex import dato_actual as dato_bitfinex
from hitbtc import dato_actual as dato_hitbtc
from config import TICKERS

n_threads = 5

BROKERS = [{
    'name': 'okex',
    'dato_actual': dato_okex
}, {
    'name': 'binance',
    'dato_actual': dato_binance
}, {
    'name': 'bitfinex',
    'dato_actual': dato_bitfinex
}, {
    'name': 'hitbtc',
    'dato_actual': dato_hitbtc
}]

data = {}

for broker in BROKERS:
    data[broker['name']] = {}


def worker(ticker, broker):
    dato_actual = broker['dato_actual']

    try:
        data[broker['name']][ticker] = dato_actual(ticker)
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

print(pd.DataFrame(data.values()))

# sp500_earnings = pd.concat(dfs)
# sp500_earnings