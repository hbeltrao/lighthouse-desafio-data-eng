import sqlite3
import csv
import os

# Import variable with the absolute path os the project, to  ensure code portability
from Job_desafio_modulo_5.config.definitions import ROOT_DIR

def fetch_orders():
    
    # Creating database connection and cursor to execute and fetch query data
    with sqlite3.connect(os.path.join(ROOT_DIR, 'data', 'Northwind_small.sqlite')) as conn:

        cur = conn.cursor()
        cur.execute("""
        select * from "Order";
        """)

        orders_data = cur.fetchall()
        orders_header = [description[0] for description in cur.description]


    # saving the result in the .csv file. using os.path.join makes sure the result path is compatible with multiple OS
    with open(os.path.join(ROOT_DIR, 'outputs', 'output_orders.csv'), 'w', encoding='UTF8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(orders_header)
        writer.writerows(orders_data)

fetch_orders()