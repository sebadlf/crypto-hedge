from db import BD_CONNECTION
from sqlalchemy import create_engine
from okex_utils import crear_tabla, dato_historico
from config import TICKERS, FIRST_ROW_DATE
from utils import get_last_row, delete_last_row
from datetime import timedelta

db_connection = create_engine(BD_CONNECTION)

crear_tabla(db_connection)

for ticker in TICKERS:

    print(ticker)

    finished = False

    while not finished:

        last_row = get_last_row(conn=db_connection, table_name='okex', ticker=ticker)

        if last_row.get('id'):
            res = delete_last_row(conn=db_connection, table_name='okex', id=last_row['id'])
            #print(res)

        since = last_row.get('time') if last_row.get('time') else FIRST_ROW_DATE

        to = since + timedelta(minutes=2999)

        df = dato_historico(ticker, 'USDT', desde=since, hasta=to, timeframe="1m")

        #print('append')

        df.to_sql('okex', db_connection, if_exists='append')

        finished = len(df) < 2






