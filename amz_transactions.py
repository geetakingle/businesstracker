#!/home/mice/anaconda3/envs/wealthtracker/bin/python

import psycopg2 as psql
import pandas as pd
import configparser
import os
from datetime import datetime, timedelta
import numpy as np
import requests


class Cashflows:
    def __init__(self, configfile):
        self.capex_cols = ['id', 'date', 'amount', 'description', 'category']
        self.opex_cols = ['id', 'date', 'amount', 'description', 'category']
        self.amz_transactions_cols = ['id',
                                      'settlement_id',
                                      'transaction_type',
                                      'sku',
                                      'order_id',
                                      'shipment_id',
                                      'marketplace_name',
                                      'amount_type',
                                      'amount_description',
                                      'amount',
                                      'quantity_purchased',
                                      'posted_date_time']

        psql_config = configparser.ConfigParser()
        psql_config.read(configfile)
        local_ip = psql_config['myip']['localip']
        current_ip = requests.get('https://api.ipify.org/').text
        location = 'outside' if local_ip != current_ip else ''
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

    def get_table_range(self, table, date_cat, fro, to):
        command = f"""SELECT * from {table} 
                        WHERE {date_cat} BETWEEN '{fro}'::date AND '{to}'::date
                        ORDER BY {date_cat}"""
        return self.execute_command(command)

    def get_cashflows(self, fro=None, to=None):
        tz = 'America/Edmonton'
        # Convert fro and to to datetime
        fro = pd.Timestamp('2021-01-01', tz=tz) if fro is None else pd.Timestamp(fro, tz=tz)
        to = pd.Timestamp(datetime.now().date(), tz=tz) if to is None else pd.Timestamp(to, tz=tz)

        # GENERATE ONE MONTH TIMEFRAMES for hist bins
        # We need to put the start and end dates to first and last of each respective month,
        # not where indicated by fro and to. Manipulate the dates to get those dates
        monthly = pd.date_range(start=fro, end=to, freq='M', tz=tz)
        # These lists hold left and right edges of bins
        dates_start = []
        dates_end = []
        # Append the first day of the month of 'fro', in case 'fro' was entered in the middle
        dates_start.append(fro - timedelta(days=(fro.day - 1)))
        for date in monthly:
            dates_end.append(date)
            dates_start.append(date + timedelta(days=1))
        # Append the last day of the month of 'to',  not 'to' itself. This will prevent incomplete cashflow calculations
        dates_end.append(pd.date_range(start=to, end=to + timedelta(days=(32 - to.day)),
                                       freq='M', tz=tz)[0])

        # RETRIEVE CASHFLOWS
        # Retrieve amz_transaction data based on calculated dates and input to DataFrame
        df_capex = pd.DataFrame(
            self.get_table_range('capex', 'date', dates_start[0], dates_end[-1]),
            columns=self.capex_cols)

        # Retrieve amz_transaction data based on calculated dates and input to DataFrame
        df_opex = pd.DataFrame(
            self.get_table_range('opex', 'date', dates_start[0], dates_end[-1]),
            columns=self.opex_cols)

        # Retrieve amz_transaction data based on calculated dates and input to DataFrame
        df_amz = pd.DataFrame(
            self.get_table_range('amz_transactions', 'posted_date_time', dates_start[0], dates_end[-1]),
            columns=self.amz_transactions_cols)
        df_amz.drop(columns='id', inplace=True)  # Drop id, it's useless

        # Covert dates to datetime type with local tz
        df_capex['date'] = pd.to_datetime(df_capex['date']).dt.tz_localize(tz)
        df_opex['date'] = pd.to_datetime(df_opex['date']).dt.tz_localize(tz)
        df_amz['posted_date_time'] = pd.to_datetime(df_amz['posted_date_time']).dt.tz_localize(tz)

        # Generate Histogram
        hist_bin = dates_start + [dates_end[-1]]  # Generate bins for histogram

        # Generate Masks and Filter Data
        mask_amz = (df_amz['amount_description'] != 'Successful charge') & \
                   (df_amz['amount_description'] != 'Previous Reserve Amount Balance') & \
                   (df_amz['amount_description'] != 'Current Reserve Amount')
        df_amz = df_amz[mask_amz]

        # Combine dates,amounts of all DataFrames into one DF
        raw_data = pd.DataFrame(columns=['date', 'amount', 'expenditure'])
        # Add in CAPEX
        df_capex['expenditure'] = 'capex'
        raw_data = pd.concat([raw_data, df_capex[['date', 'amount', 'expenditure']]])
        # Add in manual OPEX
        df_opex['expenditure'] = 'opex'
        raw_data = pd.concat([raw_data, df_opex[['date', 'amount', 'expenditure']]])
        # Add in AMZ OPEX
        df_amz.rename(columns={'posted_date_time': 'date'}, inplace=True)
        df_amz['expenditure'] = 'opex'
        raw_data = pd.concat([raw_data, df_amz[['date', 'amount', 'expenditure']]])

        return raw_data, hist_bin

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.conn.close()


class Transactions:
    def __init__(self, configfile):
        psql_config = configparser.ConfigParser()
        psql_config.read(configfile)
        local_ip = psql_config['myip']['localip']
        current_ip = requests.get('https://api.ipify.org/').text
        location = 'outside' if local_ip != current_ip else ''
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
                    # df = df.fillna('')

                    # Check if transactions already added to db
                    if self.is_settlement_id_added(next(df.itertuples()).settlement_id):
                        # File already added, go to next file
                        print(f'Duplicate Statement {each_file}')
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
    pass


if __name__ == '__main__':
    main()
