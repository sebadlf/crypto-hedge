# -*- coding: utf-8 -*-
"""
Created on Fri Dec  4 20:13:04 2020

@author: LuisaoRocks
"""
'''
import db
from sqlalchemy import create_engine
BD_CONNECTION = 'mysql+pymysql://root:password@34.66.156.85:3306/crypto-hedge'
# conexion a la DB
db_connection = create_engine(db.BD_CONNECTION)
conn = db_connection.connect()
create_table = 'select * from binance'
db_connection.execute(create_table)
'''

import mysql.connector
from mysql.connector import Error

try:
    connection = mysql.connector.connect(host='34.66.156.85',
                                         database='crypto-hedge',
                                         user='root',
                                         password='password')
    if connection.is_connected():
        db_Info = connection.get_server_info()
        print("Connected to MySQL Server version ", db_Info)
        cursor = connection.cursor()
        cursor.execute("select database();")
        record = cursor.fetchone()
        print("You're connected to database: ", record)
        sql_select_Query = "SELECT * FROM current_price"
        cursor.execute(sql_select_Query)
        records = cursor.fetchall()
        print("Seleccionamos una tabla ", records)
except Error as e:
    print("Error while connecting to MySQL", e)
finally:
    if (connection.is_connected()):
        cursor.close()
        connection.close()
        print("MySQL connection is closed")