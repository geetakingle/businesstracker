#!/home/mice/anaconda3/envs/wealthtracker/bin/python

import psycopg2 as psql
import pandas as pd
import configparser
import os
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import numpy as np
import socket


def chart_cashflows(cfs):
    plt.bar(cfs.keys(), cfs.values())
    plt.show()


class Cashflows:
    def __init__(self):
        self.accounts_cols = ['account_id', 'account_type', 'bank', 'status', 'joint']
        self.transactions_cols = ['id', 'account_id', 'transaction_date', 'category', 'description', 'amount']
        self.expense_types_cols = ['id', 'category', 'target']

        psql_config = configparser.ConfigParser()
        psql_config.read("psql_config.ini")
        self.conn = psql.connect(host=psql_config['postgresql']['host'],
                                 database=psql_config['postgresql']['database'],
                                 user=psql_config['postgresql']['user'],
                                 password=psql_config['postgresql']['password'])

    def execute_command(self, command):
        with self.conn:
            with self.conn.cursor() as curs:
                curs.execute(command)
                return curs.fetchall()

    def get_table_range(self, table, date_cat, fro, to, order_by='transaction_date'):
        command = f"""SELECT * from {table} 
                        WHERE {date_cat} > '{fro}'::date AND {date_cat} < '{to}'::date 
                        ORDER BY {order_by} ASC"""
        return self.execute_command(command)

    def get_cashflows(self, fro=None, to=None):
        # Convert fro and to to datetime
        fro = datetime.strptime('2020-01-01', '%Y-%m-%d') if fro is None else datetime.strptime(fro, '%Y-%m-%d')
        to = datetime.now().date() if to is None else datetime.strptime(to, '%Y-%m-%d')

        # We need to put the start and end dates to first and last of each respective month,
        # not where indicated by fro and to. Manipulate the dates to get those dates
        monthly = pd.date_range(start=fro, end=to, freq='M')
        dates_start = []
        dates_end = []
        # Append the first day of the month of 'fro', in case 'fro' was entered in the middle
        dates_start.append(fro - timedelta(days=(fro.day - 1)))
        for date in monthly:
            dates_end.append(date)
            dates_start.append(date + timedelta(days=1))
        # Append the last day of the month of 'to',  not 'to' itself. This will prevent incomplete cashflow calculations
        dates_end.append(pd.date_range(start=to, end=to + timedelta(days=(32 - to.day)), freq='M')[0])

        # Retrieve transaction data based on calculated dates and input to DataFrame
        df = pd.DataFrame(self.get_table_range('transactions', 'transaction_date', dates_start[0], dates_end[-1]),
                          columns=self.transactions_cols)
        df.drop('id', 1, inplace=True)  # Drop id, it's useless
        df['transaction_date'] = pd.to_datetime(df['transaction_date'])  # Covert dates to datetime type
        df.set_index('transaction_date')  # Set dates to index

        # Generate masks and get cashflows
        cashflows = {}
        for start, end in zip(dates_start, dates_end):
            # We want to mask by dates, and filter out expense_types: credit_payments, admin, ignore
            mask = (df['transaction_date'] >= start) & (df['transaction_date'] <= end) & \
                   (df['category'] != 'admin') & \
                   (df['category'] != 'credit_payments') & \
                   (df['category'] != 'ignore')

            # Create dict entry by Month Year and net cashflow
            cashflows[start.strftime('%B %Y')] = df[mask]

        return cashflows

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.conn.close()


class Transactions:
    def __init__(self):
        psql_config = configparser.ConfigParser()
        psql_config.read("psql_config.ini")
        if '192.168' not in socket.gethostbyname(socket.gethostname()):
            location = 'outside'
        conn_location = ''.join(['postgresql', location])
        self.conn = psql.connect(host=psql_config[conn_location]['host'],
                                 port=psql_config[conn_location]['port'],
                                 database=psql_config[conn_location]['database'],
                                 user=psql_config[conn_location]['user'],
                                 password=psql_config[conn_location]['password'])

    def execute_command(self, command):
        with self.conn:
            with self.conn.cursor() as curs:
                curs.execute(command)
                return curs.fetchall()

    def is_settlement_id_added(self, settlement_id):
        command = f"SELECT id from amz_settlements"
        return settlement_id in self.execute_command(command)

    def insert_transactions(self):
        """ Insert Transactions into Database using Flat File v2 in new_transactions folder
            Structure of file shall be directly as is from Amazon Settlement Records """

        try:
            all_files = os.listdir("new_statements")
            for each_file in all_files:
                file_path = os.path.join("new_statements", each_file)
                with open(file_path, 'r') as file:
                    df = pd.read_table(file)
                    df = df.set_axis([itm.replace('-', '_') for itm in list(df.keys())], axis=1)
                    #df = df.fillna('')

                    # Check if transactions already added to db
                    if self.is_settlement_id_added(next(df.itertuples()).settlement_id):
                        # File already added, go to next file
                        print(f'Duplicate record {each_file}')
                    else:
                        for line in df.itertuples():
                            if line.settlement_start_date is not np.nan:  # Record settlement id and dates
                                command = f"""INSERT INTO amz_settlements (id,start_date,end_date)
                                                VALUES ('{line.settlement_id}'::bigint,
                                                        '{line.settlement_start_date}'::timestamp,
                                                        '{line.settlement_end_date}'::timestamp)
                                                RETURNING *"""
                                self.execute_command(command)
                            elif "Payable to" in line.amount_description:  # Don't add this as it double counts
                                continue
                            else:
                                command = f"""INSERT INTO amz_transactions 
                                                (settlement_id,
                                                transaction_type,
                                                sku,
                                                order_id,
                                                shipment_id,
                                                marketplace_name,
                                                amount_type,
                                                amount_description,
                                                amount,
                                                quantity_purchased,
                                                posted_date_time)
                                              VALUES
                                                ('{line.settlement_id}'::bigint,
                                                '{line.transaction_type}',
                                                '{line.sku}',
                                                '{line.order_id}',
                                                '{line.shipment_id}',
                                                '{line.marketplace_name}',
                                                '{line.amount_type}',
                                                '{line.amount_description}',
                                                '{line.amount}'::float,
                                                '{line.quantity_purchased}'::float,
                                                '{line.posted_date_time}'::timestamp)
                                              RETURNING *"""
                                self.execute_command(command)

                # Move transaction file as its no longer needed
                # Overwrite if needed (usage of replace instead of rename)
                os.replace(os.path.abspath(file_path), os.path.join(os.path.abspath('old_statements'), each_file))

        except Exception as e:
            print(e)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.conn.close()


def main():
    meow = Transactions()
    meow.insert_transactions()
    pass


if __name__ == '__main__':
    main()
