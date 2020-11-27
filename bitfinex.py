"""
Created on Sun Nov 15 20:50:00 2020
https://docs.bitfinex.com/reference#rest-public-candles

RATE LIMIT
----------
- Market Data 30 req/min
- Trading 60 req/min

@author: nicopacheco121
"""
from sqlalchemy import create_engine
import requests
import pandas as pd
import pytz
from  keys import *
from datetime import datetime, timedelta
import time
import logging
import config
import db


def dato_historico(moneda1='BTC', moneda2='USDT', timeframe='1m', desde='datetime', hasta='vacio',
                   limit=10000, section = 'hist'):
    ''' desde=('2020-11-05') #YYYY-MM-DD
        hasta=('2020-11-09') # No es inclusive. si no se coloca nada, lo hace hasta el momento actual

        data=dato_historico(moneda1='BTC', moneda2='USDT',timeframe='1m',desde=desde,hasta=hasta)'''

    start_time = time.time()
    logging.basicConfig(level=logging.INFO, format='{asctime} {levelname} ({threadName:11s}) {message}', style='{')
    print(f'Ticker {moneda1}')

    # conexion a la DB
    db_connection = create_engine(db.BD_CONNECTION)
    conn = db_connection.connect()

    # creo la tabla
    create_table = '''

        CREATE TABLE IF NOT EXISTS `bitfinex` (
          `id` int(11) NOT NULL AUTO_INCREMENT,
          `ticker` varchar(20) DEFAULT '',
          `time` timestamp NULL DEFAULT NULL,
          `open` float(10) DEFAULT NULL,
          `high` float(10) DEFAULT NULL,
          `low` float(10) DEFAULT NULL,
          `close` float(10) DEFAULT NULL,
          `volume` float(10) DEFAULT NULL,
          PRIMARY KEY (`id`),
          UNIQUE KEY `idx_ticker_time` (`ticker`,`time`)
        ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=latin1;
        '''

    db_connection.execute(create_table)

    #Creo la variable Symbol
    if moneda2 == "USDT":
        moneda2 = "USD"
    symbol='t'+moneda1+moneda2

    # Presumo que las fechas son UTC0
    if hasta == 'vacio':
        hasta = datetime.now()
        hasta = hasta.replace(tzinfo=pytz.utc)+timedelta(days=1)
    else:
        #hasta = datetime.fromisoformat(hasta)
        hasta = hasta.replace(tzinfo=pytz.utc)+timedelta(days=1)


    #desde = datetime.fromisoformat(desde)
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

        if js==[]:
            print(f'Problema con {moneda1}')
            finished=True

        # Armo el dataframe

        df = pd.DataFrame(js)

        # Verifico que traigo mas de una fila y es algo nuevo, si no, le doy break
        if len(df) ==1:
            if df.iloc[0][0] == df_acum.iloc[-1][0]:
                break

        df_acum = df_acum.append(df, sort=False)

        contador += 1

    # Convierto los valores strings a numeros
    df_acum = df_acum.apply(pd.to_numeric,errors='ignore')

    # Renombro las columnas segun lo acordado.
    df_acum.columns = ['time', 'open', 'close', 'high', 'low', 'volume']

    # Agrego columna ticker.
    df_acum['ticker'] = moneda1

    # Ordeno columnas segun lo acordado.
    df_acum = df_acum[['ticker','time', 'open', 'high', 'low', 'close', 'volume']]

    # Borro algún posible duplicado
    df_acum = df_acum.drop_duplicates(['time'], keep='last')

    # Elimino las filas que me trajo extras en caso que existan
    df_acum = df_acum[df_acum.time < endTime]

    # Paso a timestamp el time
    df_acum['time'] = pd.to_datetime(df_acum.time, unit='ms')

    # Le mando indice de time
    df_acum.set_index('time',inplace=True)

    df_acum.to_sql(con=db_connection, name='bitfinex', if_exists='append')

    print("--- %s seconds ---" % (time.time() - start_time))

    return df_acum



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
    #print(js)
    ask_PAR=js[2]
    bid_PAR=js[0]
    return (ask_PAR,bid_PAR)



""" / / / EJECUTAR LA FUNCION / / / """

desde = datetime.utcnow() - timedelta(weeks=13)
hasta = datetime.utcnow()

# for ticker in config.TICKERS:
# #for ticker in tickers:
#     dato_historico(moneda1=ticker,desde=desde,hasta=hasta)

#print(dato_actual("BTC","USDT"))

