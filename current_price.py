import threading
from db_seba import BD_CONNECTION
from sqlalchemy import create_engine
import datetime as dt

from okex_utils import dato_actual_ponderado as dato_okex
from binance import dato_actual_ponderado as dato_binance

from utils import create_current_price_table, insert_current_price


from config import TICKERS

db_connection = create_engine(BD_CONNECTION)

create_current_price_table(db_connection)

start = dt.datetime.now()

BROKERS = [{
    'name': 'okex',
    'dato_actual': dato_okex
},
    {
    'name': 'binance',
    'dato_actual': dato_binance
}]

def worker(ticker, broker):
    dato_actual = broker['dato_actual']

    try:
        value = dato_actual(ticker, 'USDT')

        data = {
            'ticker': ticker,
            'broker': broker['name'],
            'time': dt.datetime.utcnow(),
            'ask_sum': value[0],
            'ask_volume': value[1],
            'bid_sum': value[2],
            'bid_volume': value[3]

        }

        insert_current_price(db_connection, data)

    except:
        print(f'Error en {broker["name"]} - {ticker}')

while True:

    threads = []
    for ticker in TICKERS:

        for broker in BROKERS:
            t = threading.Thread(target=worker, args=(ticker, broker))

            threads.append(t)
            t.start()

    for t in threads:
        t.join()

print(dt.datetime.now() - start)

#print(pd.DataFrame(data).sort_values("ticker"))

# sp500_earnings = pd.concat(dfs)
# sp500_earnings