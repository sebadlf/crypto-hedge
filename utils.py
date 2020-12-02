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
    CREATE TABLE if not exists `current_price` (
      `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
      `ticker` varchar(20) NOT NULL,
      `broker` varchar(20) NOT NULL,
      `time` datetime NOT NULL,
      `time_raw` datetime NOT NULL,
      `ask_ppp` float NOT NULL,
      `ask_volume` float NOT NULL,
      `bid_ppp` float DEFAULT NULL,
      `bid_volume` float DEFAULT NULL,
      PRIMARY KEY (`id`),
      KEY `idx_ticker_broker_time` (`ticker`,`broker`,`time`)
    ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=latin1;
    '''

    db_connection.execute(create_table)

def create_difference_table(db_connection):

    create_table = f'''
    CREATE TABLE if not exists `difference` (
      `id` int(11) NOT NULL AUTO_INCREMENT,
      `ticker` varchar(20) NOT NULL,
      `time` datetime NOT NULL,
      `time_raw` datetime NOT NULL,
      `broker_1` varchar(20) NOT NULL,
      `broker_2` varchar(20) NOT NULL,
      `ask_ppp_1` float NOT NULL,
      `bid_ppp_2` float DEFAULT NULL,
      `ask_volume_1` float NOT NULL,
      `bid_volume_2` float DEFAULT NULL,
      `difference` double DEFAULT NULL,
      PRIMARY KEY (`id`)
    ) ENGINE=InnoDB AUTO_INCREMENT=64 DEFAULT CHARSET=latin1;
    '''

    db_connection.execute(create_table)



def insert_current_price(db_connection, data):
    raw_time = data['time']
    time = raw_time.replace(second=0, microsecond=0)

    query = f'''
        insert current_price(ticker, broker, time, time_raw, ask_ppp, ask_volume, bid_ppp, bid_volume)
        values (
            '{data['ticker']}',
            '{data['broker']}',
            '{time.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] }',
            '{raw_time.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}',
            {data['ask_ppp']},
            {data['ask_volume']},
            {data['bid_ppp']},
            {data['bid_volume']}
        )
    '''

    db_connection.execute(query)


def insert_difference(db_connection):

    query = f'''
    insert difference (
        ticker, 
        time, 
        time_raw, 
        broker_1, 
        broker_2, 
        ask_ppp_1, 
        #ask_ppp_2, 
        #bid_ppp_1, 
        bid_ppp_2, 	
        ask_volume_1, 
        #ask_volume_2, 
        #bid_volume_1, 
        bid_volume_2,
        difference
    )
    select 
        c1.ticker, 
        c1.time, 
        c1.time_raw, 
        c1.broker as broker_1, 
        c2.broker as broker_2, 
        c1.ask_ppp as ask_ppp_1, 
        #c2.ask_ppp as ask_ppp_2, 
        #c1.bid_ppp as bid_ppp_1, 
        c2.bid_ppp as bid_ppp_2, 	
        c1.ask_volume as ask_volume_1, 
        #c2.ask_volume as ask_volume_2, 
        #c1.bid_volume as bid_volume_1, 
        c2.bid_volume as bid_volume_2,
        (1 - c1.ask_ppp / c2.bid_ppp) * 100 as difference
    from current_price c1 
    join current_price c2 
    on c1.ticker = c2.ticker
    and c1.broker != c2.broker
    and c1.time_raw = c2.time_raw
    
    where c1.time_raw = (
        select max(time_raw) from current_price cp 
    )
    '''

    db_connection.execute(query)
