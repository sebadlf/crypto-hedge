import requests
import pandas as pd
from db import BD_CONNECTION
from sqlalchemy import create_engine
engine = create_engine(BD_CONNECTION)
from datetime import datetime, timedelta

#tickers = ['BTC']

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
engine.execute(create_table)

for ticker in tickers:

    print(ticker)

    finished = False
    while not finished:

        url = f'https://okex.com/api/spot/v3/instruments/{ticker}-USDT/history/candles'

        busquedaUltimaFecha = f'SELECT `id`,`time` FROM okex WHERE `ticker` = "{ticker}" ORDER BY `time` DESC limit 0,1'
        ultimaFecha = engine.execute(busquedaUltimaFecha).fetchone()

        end = datetime.now() - timedelta(days=100)

        if (ultimaFecha):
            id = ultimaFecha[0]
            end = ultimaFecha[1]

            query_borrado = f'DELETE FROM okex WHERE `id`={id}'
            engine.execute(query_borrado)

        start = end + timedelta(minutes=300 - 1)

        params = {
            'start': start.strftime('%Y-%m-%dT%H:%M:%S.000Z'),
            'end': end.strftime('%Y-%m-%dT%H:%M:%S.000Z'),
            'granularity': 60
        }

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

        df['ticker'] = ticker

        df.set_index('time', inplace=True)

        df.to_sql('okex', engine, if_exists='append')

        finished = len(df) < 2






