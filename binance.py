# -*- coding: utf-8 -*-
"""
Created on Thu Nov 12 22:33:46 2020
https://binance-docs.github.io/apidocs/spot/en/#kline-candlestick-data

@author: LuisaoRocks
"""
import requests
import pandas as pd
import pytz
from  keys import *
from datetime import datetime, timedelta



def dato_historico(moneda1='BTC', moneda2='USDT', timeframe='1m', desde='datetime', hasta='datetime', limit=1000):
    '''desde=datetime.fromisoformat('2020-11-05') #YYYY-MM-DD
        hasta=datetime.fromisoformat('2020-11-09') # No es inclusive.
        data=bajadaSimple(moneda1='BTC', moneda2='USDT',interval='1m',desde=desde,hasta=hasta)'''

    
    #Creo la variable Symbol
    symbol=moneda1+moneda2
   
    #presumo que las fechas son UTC0
    desde=desde.replace(tzinfo=pytz.utc)
    hasta=hasta.replace(tzinfo=pytz.utc)+timedelta(days=1)
    #Llevo las variables Datetime a ms
    startTime=int(desde.timestamp()*1000)
    endTime=int(hasta.timestamp()*1000)
    df_acum=pd.DataFrame(columns=('openTime', 'open', 'high', 'low', 'close', 'volume', 'cTime',
       'qVolume', 'trades', 'takerBase', 'takerQuote', 'Ignore'))
    df=pd.DataFrame(columns=('openTime', 'open', 'high', 'low', 'close', 'volume', 'cTime',
       'qVolume', 'trades', 'takerBase', 'takerQuote', 'Ignore'))
    
    finished = False
    url = 'https://api.binance.com/api/v3/klines'
    while not finished:
       
        try:
            ultimaFecha=df.sort_values(by='openTime',ascending=True).iloc[-1]['openTime']
        except:
            ultimaFecha=startTime
        
        
        #Inicio Bajada
        params = {'symbol':symbol, 'interval':timeframe, 
                  'startTime':ultimaFecha, 'limit':limit}

        r = requests.get(url, params=params)
        js = r.json()
        
        if js==[]:
            print('Fechas no Validas')
            
        # Armo el dataframe
        cols = ['openTime','open','high','low','close','volume','cTime',
                'qVolume','trades','takerBase','takerQuote','Ignore']
        
        df = pd.DataFrame(js, columns=cols)
        df_acum=pd.concat([df_acum,df], join='inner', axis=0)
        
        if (ultimaFecha>=endTime):
            finished=True
            


    
    #Convierto los valores strings a numeros
    df_acum = df_acum.apply(pd.to_numeric)
   
    
    # Elimino columnas que no quiero
    df_acum.drop(['cTime','takerBase','takerQuote','Ignore','trades','qVolume'],axis=1,inplace=True)
    
    #Renombro las columnas segun lo acordado.
    df_acum.columns=['time','open','high','low','close','volume']
    
    #Elimino las filas que me trajo extras en caso que existan
    df_acum=df_acum[df_acum.time<endTime]
    
    # Le mando indice de time
    df_acum.index = pd.to_datetime(df_acum.time, unit='ms')
    
    return df_acum

def dato_actual(moneda1='BTC', moneda2='USDT'):
    '''data=dato_actual(moneda1='BTC', moneda2='USDT')'''
    #Creo la variable Symbol
    symbol=moneda1+moneda2

    url = 'https://api.binance.com/api/v3/depth'
    params = {'symbol':symbol,'limit':5}
    r = requests.get(url, params=params)
    js = r.json()
    ask_PAR=js.get('asks')[0][0]   
    bid_PAR=js.get('bids')[0][0]
    return (ask_PAR,bid_PAR)


