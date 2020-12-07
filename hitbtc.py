"""
Created on Sun Nov 15 12:00:00 2020
https://api.hitbtc.com/

RATE LIMIT
----------
- Market Data 100 per second for ip
- Trading 300 per second for one user
- Other requests, including Trading history, the limit is 10 requests per second for one user

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
import db
import okex_utils
import config
from requests.auth import HTTPBasicAuth


def GuardoDB(data,ticker,broker='hitbtc'):
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
    

def dato_historico(moneda1='BTC', moneda2='USD', period='M1', sort='ASC',
                   desde=datetime.utcnow() - timedelta(weeks=13),
                   hasta=datetime.utcnow(),limit='1000'):

    """
    Inputs
    -----
    Pediodos: M1 (one minute), M3, M5, M15, M30, H1 (one hour), H4, D1 (one day), D7, 1M (one month)
    Desde y hasta: string YYYY-MM-DD

    """
    start_time = time.time()
    logging.basicConfig(level=logging.INFO, format='{asctime} {levelname} ({threadName:11s}) {message}', style='{')
    print(f'Ticker {moneda1}')

    # Creo la variable Symbol
    if moneda2 == 'USDT' and moneda1 != 'XRP':
        moneda2 = 'USD'

    symbol = moneda1 + moneda2

    # presumo que las fechas son UTC0
    desde = desde.replace(tzinfo=pytz.utc)
    hasta = datetime.strptime(datetime.strftime(hasta, '%Y-%m-%d %H:%M'), '%Y-%m-%d %H:%M')
    hasta = hasta.replace(tzinfo=pytz.utc) + timedelta(days=1)

    # Inicializo el df acumulado
    df_acum = pd.DataFrame(columns=('timestamp', 'open', 'close', 'min', 'max', 'volume', 'volumeQuote'))

    finished = False
    url= f'https://api.hitbtc.com/api/2/public/candles/{symbol}'
    contador = 0

    while not finished:
        try:
            ultimaFecha = df_acum.iloc[-1]['timestamp']
        except:
            ultimaFecha = desde

        logging.info(f'Bajada n° {contador}')

        # Inicio Bajada
        params = {'period' : period,'sort' : sort,
                  'from' : ultimaFecha, 'till' : hasta,
                  'limit' : limit}

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

        df_acum = df_acum.append(df,sort=False)

        # verifico si ya llego a la fecha solicitada
        desde_verificar = ultimaFecha

        if contador != 0:
            desde_verificar = desde_verificar.replace("Z","")
            desde_verificar = datetime.strptime(ultimaFecha, '%Y-%m-%dT%H:%M:%S.%fZ')
            desde_verificar = int(desde_verificar.timestamp() * 1000)
            hasta_verificar = int(hasta.timestamp() * 1000)

            if (desde_verificar >= hasta_verificar):
                finished = True
        else:
            if (desde_verificar >= hasta):
                finished = True

        contador+= 1

    # Convierto los valores strings a numeros
    df_acum = df_acum.apply(pd.to_numeric,errors='ignore')

    # Elimino columnas que no quiero
    df_acum.drop(['volumeQuote'], axis=1, inplace=True)

    # Renombro las columnas segun lo acordado.
    df_acum.columns = ['time', 'open','close', 'low', 'high', 'volume']

    # Agrego columna ticker.
    df_acum['ticker'] = moneda1

    # Ordeno columnas segun lo acordado.
    df_acum = df_acum[['ticker','time', 'open', 'high', 'low', 'close', 'volume']]

    # Borro algún posible duplicado
    df_acum = df_acum.drop_duplicates(['time'], keep='last')

    # Paso a timestamp el time
    df_acum['time'] = pd.to_datetime(df_acum.time)

    # Elimino las filas que me trajo extras en caso que existan
    try:
        df_acum = df_acum[df_acum.time < hasta]
    except:
        pass
    # Le mando indice de time
    df_acum.set_index('time',inplace=True)

    print("--- %s seconds ---" % (time.time() - start_time))

    return df_acum


def guardado_historico(moneda1='BTC', moneda2='USDT',timeframe='M1',desde=datetime.utcnow() - timedelta(weeks=13),
                       hasta=datetime.utcnow(),broker='hitbtc'):
    
    try:
        # conexion a la DB
        db_connection = create_engine(db.BD_CONNECTION)
        #conn = db_connection.connect()
    
        #Busco el ultimo dato guardado.
        busquedaUltimaFecha = f'SELECT `id`,`time` FROM {broker} WHERE `ticker` = "{moneda1}" ORDER BY `time` DESC limit 0,1'
        ultimaFecha = db_connection.execute(busquedaUltimaFecha).fetchone()
    
        #Si encuentro un ultimo registro, lo elimino
        if (ultimaFecha):
            id = ultimaFecha[0]
            query_borrado = f'DELETE FROM {broker} WHERE `id`={id}'
            db_connection.execute(query_borrado)
            desde=ultimaFecha[1]

    except:
        pass

    #Bajo Informacion.
    data=dato_historico(moneda1=moneda1, moneda2=moneda2,period=timeframe,desde=desde,hasta=hasta)
    
    #Guardo en DB
    GuardoDB(data,moneda1)



def dato_actual(moneda1='BTC', moneda2='USD'):
    '''data=dato_actual(moneda1='BTC', moneda2='USDT')'''
    # Creo la variable Symbol
    if moneda2 == 'USDT' and moneda1 != 'XRP':
        moneda2 = 'USD'

    symbol=moneda1+moneda2

    url = f'https://api.hitbtc.com/api/2/public/ticker/{symbol}'
    r = requests.get(url)
    js = r.json()

    ask_PAR=js['ask']
    bid_PAR=js['bid']
    return (ask_PAR,bid_PAR)


def dato_actual_ponderado(moneda1, moneda2="USDT",profundidad = 5, precision='R0',len = 5):
    """ Ratelimit: 100 requests per second for one IP """

    # Creo la variable Symbol
    if moneda2 == 'USDT' and moneda1 != 'XRP':
        moneda2 = 'USD'

    symbol = moneda1 + moneda2

    # Requests
    url = f'https://api.hitbtc.com/api/2/public/orderbook/{symbol}'
    params = {'limit': len}
    r = requests.get(url,params=params)
    js = r.json()

    prod_bid_vol = 0
    vol_bid = 0
    prod_ask_vol = 0
    vol_ask = 0

    for i in range(len):

        precio_ask = float(js['ask'][i]['price'])
        vol_ask_i = float(js['ask'][i]['size'])
        precio_bid = float(js['bid'][i]['price'])
        vol_bid_i = float(js['bid'][i]['size'])

        if vol_ask_i >0:
            vol_ask += vol_ask_i
            prod_ask_vol += precio_ask * vol_ask_i
        if vol_bid_i >0:
            vol_bid += vol_bid_i
            prod_bid_vol += precio_bid * vol_bid_i

    ppp_bid = prod_bid_vol / vol_bid if vol_bid > 0 else None
    ppp_ask = prod_ask_vol / vol_ask if vol_ask > 0 else None

    #print(js)

    return (ppp_ask, vol_ask, ppp_bid, vol_bid)


""" / / / EJECUTAR LA FUNCION / / / """


"""inicio = time.time()
for ticker in config.TICKERS:

   #dato_historico(moneda1=ticker,moneda2='USDT')
   guardado_historico(moneda1=ticker,moneda2='USDT')

print("--- %s seconds ---" % (time.time() - inicio))"""

#print(dato_actual("BCH","USDT"))





""" / / / ALGUNA FUNCION EXTRA / / / """

def symbols():
    """ Retorna un dataframe con todos los pares e informacion sobre estos """
    url = "https://api.hitbtc.com/api/2/public/symbol"
    r = requests.get(url)
    js = r.json()
    df = pd.DataFrame(js).set_index('id')
    return df

"""hola = symbols()
agrupado = hola.groupby("quoteCurrency").count()
print(agrupado)"""

'''antes de poder usar esta funcion, debemos tener
autorizados en https://hitbtc.com/settings/api-keys  los derechos de acceso.
CUIDADO, antes de poder usarlo, tambien hay que verificar en el correo, pq
te mandan un mail para confirmar.
dentro del archivo keys.py, debemos agrear las dos variables HITBTC_KEY y
HITBTC_SECRET'''

def estado_cuenta():
    url='https://api.hitbtc.com/api/2/account/balance'
    r=requests.get(url, auth=HTTPBasicAuth(HITBTC_KEY,HITBTC_SECRET)).json()
    
    print(r)

#estado_cuenta()
