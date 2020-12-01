import threading
from db_seba import BD_CONNECTION
from sqlalchemy import create_engine
import datetime as dt

from okex_utils import dato_actual_ponderado as dato_okex
from binance import dato_actual_ponderado as dato_binance
from bitfinex import dato_actual_ponderado as dato_bitfinex



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
},
    {
    'name': 'bitfinex',
    'dato_actual': dato_bitfinex
}]

def worker(ticker, broker, time):
    dato_actual = broker['dato_actual']

    try:
        value = dato_actual(ticker, 'USDT')

        data = {
            'ticker': ticker,
            'broker': broker['name'],
            'time': time,
            'ask_ppp': value[0],
            'ask_volume': value[1],
            'bid_ppp': value[2],
            'bid_volume': value[3]

        }

        insert_current_price(db_connection, data)

    except:
        print(f'Error en {broker["name"]} - {ticker}')

while True:

    for ticker in TICKERS:
        threads = []

        time = dt.datetime.utcnow()

        for broker in BROKERS:

            t = threading.Thread(target=worker, args=(ticker, broker, time))

            threads.append(t)
            t.start()

        for t in threads:
            t.join()

print(dt.datetime.now() - start)

#print(pd.DataFrame(data).sort_values("ticker"))

# sp500_earnings = pd.concat(dfs)
# sp500_earnings