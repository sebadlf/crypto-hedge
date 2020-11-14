import requests
import pandas as pd
from db import BD_CONNECTION
from sqlalchemy import create_engine
engine = create_engine(BD_CONNECTION)
from datetime import datetime, timedelta

def dato_historico(moneda1, moneda2, desde=None, hasta=None, timeframe="1m"):
    MAX_WINDOW = 300

    time_window = timedelta(minutes=MAX_WINDOW)
    time_unit = timedelta(minutes=1)
    granularity = 60

    if timeframe == "1d":
        granularity = 86400
        time_window = timedelta(days=MAX_WINDOW)
        time_unit = timedelta(days=1)
    elif timeframe == "1h":
        granularity = 3600
        time_window = timedelta(hours=MAX_WINDOW)
        time_unit = timedelta(hours=1)

    if desde and (hasta is None):
        hasta = desde + time_window - time_unit

    if (desde is None) and hasta:
        desde = hasta - time_window + time_unit

    if (desde is None) and (hasta is None):
        return dato_historico_download(moneda1, moneda2, desde, hasta, granularity)

    finish = False

    result = []

    while not finish:
        hasta_param = desde + time_window - time_unit

        if (hasta_param > hasta):
            hasta_param = hasta
            finish = True

        partial_df = dato_historico_download(moneda1, moneda2, desde, hasta_param, granularity)

        result.append(partial_df)

        desde = desde + time_window

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

    ask = float(data['asks'][0][1]) if len(data['asks']) else None
    bid = float(data['bids'][0][1]) if len(data['bids']) else None

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


# Test dato_historico

# desde = datetime(2020, 11, 11, 0, 0, 0)
# hasta = datetime(2020, 11, 14, 0, 0, 0)
#
# print(dato_historico("BTC", "USDT", desde, hasta, timeframe="1m"))


# Test dato_actual

#print(dato_actual('BTC', 'USDT'))
