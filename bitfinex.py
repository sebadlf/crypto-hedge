"""
Created on Sun Nov 15 20:50:00 2020
https://docs.bitfinex.com/reference#rest-public-candles

RATE LIMIT
----------
- Market Data 30 req/min
- Trading 60 req/min
- Book 90 req/min

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
from requests.auth import HTTPBasicAuth
import hashlib
import json

def guardoDB(data,ticker,broker='bitfinex'):

    # conexion a la DB
    db_connection = create_engine(db.BD_CONNECTION)
    conn = db_connection.connect()

    # creo la tabla
    create_table = f'''
        CREATE TABLE IF NOT EXISTS `{broker}` (
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
    data.to_sql(con=db_connection, name=broker, if_exists='append')


def dato_historico(moneda1='BTC', moneda2='USDT', timeframe='1m', desde=datetime.utcnow() - timedelta(weeks=24),
                   hasta='vacio',
                   limit=10000, section = 'hist'):
    ''' desde=('2020-11-05') #YYYY-MM-DD
        hasta=('2020-11-09') # No es inclusive. si no se coloca nada, lo hace hasta el momento actual

        data=dato_historico(moneda1='BTC', moneda2='USDT',timeframe='1m',desde=desde,hasta=hasta)'''


    logging.basicConfig(level=logging.INFO, format='{asctime} {levelname} ({threadName:11s}) {message}', style='{')
    print(f'Ticker {moneda1}')

    #Creo la variable Symbol
    if moneda2 == "USDT":
        moneda2 = "USD"
    if moneda1 == 'BCH':
        moneda1_ok = 'BCHN:'

    try:
        symbol='t'+moneda1_ok+moneda2
    except:
        symbol='t'+moneda1+moneda2

    # Presumo que las fechas son UTC0
    if hasta == 'vacio':
        hasta = datetime.now()
        hasta = hasta.replace(tzinfo=pytz.utc)+timedelta(minutes=1)
    else:
        #hasta = datetime.fromisoformat(hasta)
        hasta = hasta.replace(tzinfo=pytz.utc)+timedelta(minutes=1)

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
        params = {'limit': limit, 'start': ultimaFecha, 'end' : endTime, 'sort': '1'}

        r = requests.get(url, params=params)
        js = r.json()

        if js==[]:
            print(f'Problema con {moneda1}')
            finished=True

        # Armo el dataframe
        df = pd.DataFrame(js)

        # Verifico que traigo mas de una fila y es algo nuevo, si no, le doy break
        if len(df) ==1:
            try:
                if df.iloc[0][0] == df_acum.iloc[-1][0]:
                    break
            except:
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
    try:
        df_acum = df_acum[df_acum.time < hasta]
    except:
        pass

    # Paso a timestamp el time
    df_acum['time'] = pd.to_datetime(df_acum.time, unit='ms')

    # Le mando indice de time
    df_acum.set_index('time',inplace=True)

    return df_acum


def guardado_historico(moneda1='BTC', moneda2='USDT',timeframe='1m',
                       desde=datetime.utcnow() - timedelta(weeks=24), hasta=datetime.utcnow(),
                       broker='bitfinex'):

    start_time = time.time()

    try:
        # conexion a la DB
        db_connection = create_engine(db.BD_CONNECTION)
        conn = db_connection.connect()

        # Busco el ultimo dato guardado.
        busquedaUltimaFecha = f'SELECT `id`,`time` FROM {broker} WHERE `ticker` = "{moneda1}" ORDER BY `time` DESC limit 0,1'
        ultimaFecha = db_connection.execute(busquedaUltimaFecha).fetchone()

        # Si encuentro un ultimo registro, lo elimino
        if (ultimaFecha):
            id = ultimaFecha[0]
            query_borrado = f'DELETE FROM {broker} WHERE `id`={id}'
            db_connection.execute(query_borrado)
            desde = ultimaFecha[1]
    except:
        pass

    # Bajo Informacion.
    data = dato_historico(moneda1=moneda1, moneda2=moneda2, timeframe=timeframe, desde=desde, hasta=hasta)

    # Guardo en DB
    guardoDB(data=data, ticker=moneda1,broker=broker)

    return print("--- %s seconds ---" % (time.time() - start_time))


def dato_actual(moneda1='BTC', moneda2='USD'):
    '''data=dato_actual(moneda1='BTC', moneda2='USDT')'''
    #Creo la variable Symbol
    if moneda2 == "USDT":
        moneda2 = "USD"
    if moneda1 == 'BCH':
        moneda1_ok = 'BCHN:'

    try:
        symbol='t'+moneda1_ok+moneda2
    except:
        symbol='t'+moneda1+moneda2

    url = f'https://api-pub.bitfinex.com/v2/ticker/{symbol}'
    r = requests.get(url)
    js = r.json()

    ask_PAR=js[2]
    bid_PAR=js[0]
    return (ask_PAR,bid_PAR)


def dato_actual_ponderado(moneda1, moneda2="USDT",profundidad = 5, precision='R0',len = 25):
    """ Ratelimit: 90 req/min """

    # Creo la variable Symbol
    if moneda2 == "USDT":
        moneda2 = "USD"
    if moneda1 == 'BCH':
        moneda1_ok = 'BCHN:'
    try:
        symbol = 't' + moneda1_ok + moneda2
    except:
        symbol = 't' + moneda1 + moneda2

    # Requests
    url = f'https://api-pub.bitfinex.com/v2/book/{symbol}/{precision}'
    params = {'len': len}
    r = requests.get(url,params=params)
    js = r.json()

    prod_bid_vol = 0
    vol_bid = 0
    prod_ask_vol = 0
    vol_ask = 0

    for i in range(len*2):
        price = js[i][1]
        volume = js[i][2]

        #primero los bid
        if i < profundidad:
            if volume >0:
                prod_bid_vol+=price*volume
                vol_bid+=volume

        #despues los ask
        if i >24 and i<=(24+profundidad):
            if volume <0:
                prod_ask_vol += price * -volume
                vol_ask += -volume

    ppp_bid = prod_bid_vol / vol_bid if vol_bid > 0 else None
    ppp_ask = prod_ask_vol / vol_ask if vol_ask > 0 else None

    return (ppp_ask, vol_ask, ppp_bid, vol_bid)



""" / / / EJECUTAR LA FUNCION / / / """

#for ticker in config.TICKERS:
#    guardado_historico(moneda1=ticker)
#guardado_historico(moneda1='ETC')

#print(dato_actual_ponderado("BTC","USDT"))


'''https://docs.bitfinex.com/docs/rest-auth'''
def estado_cuenta():

    apiKey=BITFINEX_KEY
    apiSecret=BITFINEX_SECRET
    url='https://api.bitfinex.com/v2/auth/r/summary'
    data = {}
    nonce=str(datetime.now().timestamp()*1000)
    sig=str(hashlib.sha384(str(apiSecret).encode('utf-8')))
    headers = {'Content-Type': 'application/json','bfx-nonce': nonce,
               'bfx-apikey': apiKey,'bfx-signature': sig}
    r = requests.post(url, data=json.dumps(data), headers=headers).json()
            
    print(r)
# No esta funcionando, se lo pregunte a nacho
#estado_cuenta()

if __name__ == '__main__':
    inicio = time.time()

    hola = dato_historico('BTC')
    print(hola)

    print("--- %s seconds ---" % (time.time() - inicio))
