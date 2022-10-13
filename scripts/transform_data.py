import os

import pandas as pd

# Import variable with the absolute path os the project, to  ensure code portability
from Job_desafio_modulo_5.config.definitions import ROOT_DIR


def transform_data():

    # loading data to be merged and transformed
    orders = pd.read_csv(os.path.join(ROOT_DIR, 'outputs', 'output_orders.csv'))
    order_details = pd.read_csv(os.path.join(ROOT_DIR, 'outputs', 'output_order_details.csv'))

    # creating a column OrderId as type object to be used as key of the join
    orders['OrderId'] = orders['Id'].astype('object')
    

    joined_data = order_details.merge(orders, how='left', on='OrderId')

    # Filtering to get only the data of interest (in this case, all orders shipped to Rio de Janeiro)
    filtered_data = joined_data[joined_data['ShipCity']=='Rio de Janeiro']['Quantity'].sum()

    # saving the result in the .csv file. using os.path.join makes sure the result path is compatible with multiple OS
    with open(os.path.join(ROOT_DIR, 'outputs', 'count.txt'), 'w') as file:
        file.write(str(filtered_data))

transform_data()


