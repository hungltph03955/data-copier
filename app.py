import pandas as pd
import os
from read import get_json_reader
from write import load_db_table
import sys


def process_table(BASE_DIR, conn, table_name):
    json_reader = get_json_reader(BASE_DIR, table_name)
    for df in json_reader:
        load_db_table(df, conn, table_name, df.columns[0])


def main():
    # The file name is hardcoded and assigned to fp.
    # fp = '/Users/hungsuu/Documents/Data-engineer/Research/data/retail_db_json/order_items/part-r-00000-6b83977e-3f20-404b-9b5f-29376ab1419e'
    # df = pd.read_json(fp, lines=True)
    # df.count()
    # print(df.count())
    # df.describe()
    # print(df.describe())
    # df.columns
    # df.dtypes
    # print(df.dtypes)
    # df[['order_item_order_id', 'order_item_subtotal']]
    # df[df['order_item_order_id'] == 2]

    # json_reader = pd.read_json(fp, lines=True, chunksize=1000)
    # for idx, df in enumerate(json_reader):
    #     print(f'Number of records in chunk with index {idx} is {df.shape[0]}')

    # import data
    conn = 'postgresql://retail_user:itversity@localhost:5452/retail_db'
    BASE_DIR = '/Users/hungsuu/Documents/Data-engineer/Research/data/retail_db_json'
    table_name = 'departments'
    # file_name = os.listdir(f'{BASE_DIR}/{table_name}')[0]
    # fp = f'{BASE_DIR}/{table_name}/{file_name}'
    # df = pd.read_json(fp, lines=True)
    # df.to_sql(table_name, conn, if_exists='append', index=False)

    # loading data
    query = 'SELECT * FROM departments'
    df = pd.read_sql(
        query,
        conn
    )
    # print(df)
    df.count()
    # print(df.count())
    pd.read_sql(
        'SELECT count(1) FROM departments',
        conn
    )
    # print(pd)

    table_name = 'orders'
    # file_name = os.listdir(f'{BASE_DIR}/{table_name}')[0]
    # fp = f'{BASE_DIR}/{table_name}/{file_name}'
    # print(fp)
    # json_reader = pd.read_json(fp, lines=True, chunksize=1000)
    # print(json_reader)

    # for df in json_reader:
    #     min_key = df['order_id'].min()
    #     max_key = df['order_id'].max()
    # df.to_sql(table_name, conn, if_exists='append', index=False)
    # print(f'Processed {table_name} with in the range of {min_key} and {max_key}')

    query = 'SELECT * FROM orders'
    df = pd.read_sql(
        query,
        conn
    )
    # print(df)

    BASE_DIR = os.environ.get('BASE_DIR')
    # table_name = os.environ.get('TABLE_NAME')
    table_names = sys.argv[1].split(',')
    configs = dict(os.environ.items())
    conn = f'postgresql://{configs["DB_USER"]}:{configs["DB_PASS"]}@{configs["DB_HOST"]}:{configs["DB_PORT"]}/{configs["DB_NAME"]}'
    for table_name in table_names:
        process_table(BASE_DIR, conn, table_name)


if __name__ == "__main__":
    main()
