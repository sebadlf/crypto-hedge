def get_last_row(conn, table_name, ticker):
    last_row_query = f'SELECT `id`,`time` FROM {table_name} WHERE `ticker` = "{ticker}" ORDER BY `time` DESC limit 0,1'
    last_row = conn.execute(last_row_query).fetchone()

    last_row = {
        'id': last_row[0] if last_row else None,
        'time': last_row[1] if last_row else None
    }

    return last_row



def delete_last_row(conn, table_name, id):
    query_borrado = f'DELETE FROM {table_name} WHERE `id`={id}'
    return conn.execute(query_borrado)

def create_current_price_table(db_connection):

    create_table = f'''
    CREATE TABLE IF NOT EXISTS `current_price` (
      `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
      `ticker` varchar(20) NOT NULL,
      `broker` varchar(20) NOT NULL,
      `time` datetime NOT NULL,
      `time_raw` datetime NOT NULL,
      `ask_sum` float NOT NULL,
      `ask_volume` float NOT NULL,
      `bid_sum` float DEFAULT NULL,
      `bid_volume` float DEFAULT NULL,
      PRIMARY KEY (`id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=latin1;
    '''

    db_connection.execute(create_table)

def insert_current_price(db_connection, data):
    time = data['time']
    raw_time = time.replace(second=0, microsecond=0)

    query = f'''
        insert current_price(ticker, broker, time, time_raw, ask_sum, ask_volume, bid_sum, bid_volume)
        values (
            '{data['ticker']}',
            '{data['broker']}',
            '{ time.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] }',
            '{raw_time.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}',
            {data['ask_sum']},
            {data['ask_sum']},
            {data['bid_sum']},
            {data['bid_volume']}
        )
    '''

    db_connection.execute(query)