from amz_transactions import Transactions
import pandas as pd
from statsmodels.tsa.api import ExponentialSmoothing, SimpleExpSmoothing, Holt
from statsmodels.tsa.seasonal import seasonal_decompose

with Transactions(configfile='psql_businessfinances.ini') as db:
    command = """SELECT CAST(posted_date_time AS DATE), sum(amount) FROM amz_transactions 
                    WHERE amount_description='Principal' AND amount_type='ItemPrice' 
                    GROUP BY CAST(posted_date_time AS DATE) 
                    ORDER BY posted_date_time"""
    df = pd.DataFrame(db.execute_command(command), columns=['Date', 'Amount'])

    # Cleanup index
    df['Date'] = pd.to_datetime(df['Date'])
    df.set_index('Date', inplace=True)

    # Reindex daily to account for missing days where no sales; i.e. amount = 0
    df = df.asfreq('D', fill_value=0.0)

    #decompose_results = seasonal_decompose(df)

    df['SingleHWES'] = SimpleExpSmoothing(df['Amount']).fit()
