import requests
import pandas as pd
from db import BD_CONNECTION
from sqlalchemy import create_engine
engine = create_engine(BD_CONNECTION)
from datetime import datetime, timedelta

def dato_historico(moneda1, moneda2, desde, hasta=None, timeframe="1m"):
    MAX_WINDOW = 300

    time_window = timedelta(minutes=MAX_WINDOW)
    granularity = 60

    if timeframe == "1d":
        granularity = 86400
        time_window = timedelta(days=MAX_WINDOW)
    elif timeframe == "1h":
        granularity = 3600
        time_window = timedelta(hours=MAX_WINDOW)

    finish = False

    result = []

    while not finish:

        hasta_param = desde + time_window

        if hasta_param > hasta:
            hasta_param = hasta
            finish = True

        partial_df = dato_historico_download(moneda1, moneda2, desde, hasta_param, granularity)

        result.append(partial_df)

        desde = desde + time_window

        finish = finish or (desde >= hasta) or (desde >= datetime.utcnow())

    df = pd.concat(result)

    return df

def dato_historico_download(moneda1, moneda2, desde=None, hasta=None, granularity=60):

    url = f'https://okex.com/api/spot/v3/instruments/{moneda1}-{moneda2}/history/candles'

    params = {
        'granularity': granularity
    }

    if desde:
        params['end'] = desde.strftime('%Y-%m-%dT%H:%M:%S.000Z')

    if hasta:
        params['start'] = hasta.strftime('%Y-%m-%dT%H:%M:%S.000Z')

    print(params)

    r = requests.get(url, params=params)
    js = r.json()
    df = pd.DataFrame(js)

    df.columns = ['time', 'open', 'high', 'low', 'close', 'volume']

    df.time = pd.to_datetime(df.time)
    df.open = df.open.astype(float)
    df.high = df.high.astype(float)
    df.low = df.low.astype(float)
    df.close = df.close.astype(float)
    df.volume = df.volume.astype(float)

    df['ticker'] = moneda1

    df.set_index('time', inplace=True)
    df.sort_index(inplace=True)

    return df

def dato_actual(moneda1, moneda2="USDT"):
    data = dato_actual_download(moneda1, moneda2, size=1)

    ask = float(data['asks'][0][0]) if len(data['asks']) else None
    bid = float(data['bids'][0][0]) if len(data['bids']) else None

    return (ask, bid)

def dato_actual_download(moneda1, moneda2="USDT", size = None, depth= None):
    url = f'https://okex.com/api/spot/v3/instruments/{moneda1}-{moneda2}/book'

    params = {}

    if size:
        params['size'] = size

    if depth:
        params['depth'] = size

    r = requests.get(url, params=params)
    js = r.json()

    return js # TUPLA(ask_PAR, bid_PAR)

def dato_actual_ponderado(moneda1, moneda2="USDT", size=5):
    data = dato_actual_download(moneda1, moneda2, size=size)

    sum_ask = 0
    volume_ask = 0
    sum_bid = 0
    volume_bid = 0

    ask = data['asks']
    bid = data['bids']

    for i in range(size):
        if len(ask) >= size:
            price = float(ask[i][0])
            volume = float(ask[i][1])

            sum_ask += price * volume
            volume_ask += volume

        if len(bid) >= size:
            price = float(bid[i][0])
            volume = float(bid[i][1])

            sum_bid += price * volume
            volume_bid += volume

    ppp_ask = sum_ask / volume_ask if volume_ask > 0 else None
    ppp_bid = sum_bid / volume_bid if volume_bid > 0 else None

    return (ppp_ask, volume_ask, ppp_bid, volume_bid)

# Test dato_historico

# desde = datetime(2020, 11, 11, 0, 0, 0)
# hasta = datetime(2020, 11, 14, 0, 0, 0)
#
# print(dato_historico("BTC", "USDT", desde, hasta, timeframe="1m"))


# Test dato_actual

#print(dato_actual('BTC', 'USDT'))


def crear_tabla(db_connection):
    create_table = '''
    
    CREATE TABLE IF NOT EXISTS `okex` (
      `id` int(11) NOT NULL AUTO_INCREMENT,
      `ticker` varchar(20) DEFAULT '',
      `time` timestamp NULL DEFAULT NULL,
      `open` double DEFAULT NULL,
      `high` double DEFAULT NULL,
      `low` double DEFAULT NULL,
      `close` double DEFAULT NULL,
      `volume` double DEFAULT NULL,
      PRIMARY KEY (`id`),
      UNIQUE KEY `idx_ticker_time` (`ticker`,`time`)
    ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=latin1;
    '''

    db_connection.execute(create_table)


