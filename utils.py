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