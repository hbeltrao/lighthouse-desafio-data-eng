import sqlite3
import csv
import os

# Import variable with the absolute path os the project, to  ensure code portability
from Job_desafio_modulo_5.config.definitions import ROOT_DIR

def fetch_order_details():

    # Creating database connection and cursor to execute and fetch query data
    with sqlite3.connect(os.path.join(ROOT_DIR, 'data', 'Northwind_small.sqlite')) as conn:

        cur = conn.cursor()
        cur.execute("""
        select * from "OrderDetail";
        """)

        order_details_data = cur.fetchall()
        order_details_header = [description[0] for description in cur.description]

        
    # saving the result in the .csv file. using os.path.join makes sure the result path is compatible with multiple OS
    with open(os.path.join(ROOT_DIR, 'outputs', 'output_order_details.csv'), 'w', encoding='UTF8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(order_details_header)
        writer.writerows(order_details_data)

fetch_order_details()
