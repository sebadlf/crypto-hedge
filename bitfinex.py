"""
Created on Sun Nov 15 20:50:00 2020
https://docs.bitfinex.com/reference#rest-public-candles

RATE LIMIT
----------
- Market Data 30 req/min
- Trading 60 req/min

@author: nicopacheco121
"""

import requests
import pandas as pd
import pytz
from  keys import *
from datetime import datetime, timedelta
import time
import logging

def dato_historico(moneda1='BTC', moneda2='USDT', timeframe='1m', desde='datetime', hasta='vacio', limit=10000, section = 'hist'):
    ''' desde=('2020-11-05') #YYYY-MM-DD
        hasta=('2020-11-09') # No es inclusive. si no se coloca nada, lo hace hasta el momento actual

        data=dato_historico(moneda1='BTC', moneda2='USDT',timeframe='1m',desde=desde,hasta=hasta)'''

    start_time = time.time()
    logging.basicConfig(level=logging.INFO, format='{asctime} {levelname} ({threadName:11s}) {message}', style='{')

    #Creo la variable Symbol
    if moneda2 == "USDT":
        moneda2 = "USD"
    symbol='t'+moneda1+moneda2

    # Presumo que las fechas son UTC0
    if hasta == 'vacio':
        hasta = datetime.now()
        hasta = hasta.replace(tzinfo=pytz.utc)
    else:
        hasta = datetime.fromisoformat(hasta)
        hasta = hasta.replace(tzinfo=pytz.utc)


    desde = datetime.fromisoformat(desde)
    desde = desde.replace(tzinfo=pytz.utc)

    # Llevo las variables Datetime a ms
    startTime = int(desde.timestamp() * 1000)
    endTime = int(hasta.timestamp() * 1000)

    # Inicializo el df acumulado
    df_acum = pd.DataFrame(columns=(0, 1, 2, 3, 4, 5))

    finished = False
    url = f'https://api-pub.bitfinex.com/v2/candles/trade:{timeframe}:{symbol}/{section}'

    contador = 0
    while not finished:

        try:
            ultimaFecha = df_acum.iloc[-1][0]
        except:
            ultimaFecha = startTime

        if (ultimaFecha >= endTime):
            break

        # Inicio Bajada
        logging.info(f'Bajada n° {contador}')
        params = {'limit': limit, 'start': ultimaFecha, 'end' : hasta, 'sort': '1'}

        r = requests.get(url, params=params)
        js = r.json()

        # Armo el dataframe

        df = pd.DataFrame(js)
        df_acum = df_acum.append(df, sort=False)

        contador+= 1

    # Convierto los valores strings a numeros
    df_acum = df_acum.apply(pd.to_numeric,errors='ignore')

    # Renombro las columnas segun lo acordado.
    df_acum.columns = ['time', 'open', 'close', 'high', 'low', 'volume']

    # Ordeno columnas segun lo acordado.
    df_acum = df_acum[['time', 'open', 'high', 'low', 'close', 'volume']]

    # Elimino las filas que me trajo extras en caso que existan
    df_acum = df_acum[df_acum.time < endTime]

    # Paso a timestamp el time
    df_acum['time'] = pd.to_datetime(df_acum.time, unit='ms')

    # Le mando indice de time
    df_acum.set_index('time',inplace=True)

    print("--- %s seconds ---" % (time.time() - start_time))

    return df_acum

#ver = dato_historico('BTC','USDT',desde='2020-11-15')
#print(ver)


def dato_actual(moneda1='BTC', moneda2='USD'):
    '''data=dato_actual(moneda1='BTC', moneda2='USDT')'''
    #Creo la variable Symbol
    if moneda2 == "USDT":
        moneda2 = "USD"
    symbol='t'+moneda1+moneda2

    url = f'https://api-pub.bitfinex.com/v2/ticker/{symbol}'
    params = {'simbol':symbol}
    #r = requests.get(url,params=params)
    r = requests.get(url)
    js = r.json()
    print(js)
    ask_PAR=js[2]
    bid_PAR=js[0]
    return (ask_PAR,bid_PAR)

#print(dato_actual("BTC","USDT"))