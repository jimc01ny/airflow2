from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import yfinance as yf

def download_stock_prices():
    # Define the list of stocks you want to download data for
    stocks = ['AAPL', 'GOOGL', 'MSFT']

    # Set the start and end dates for the data
    end_date = datetime.now()
    start_date = end_date - timedelta(minutes=5)

    # Download data for each stock
    for stock in stocks:
        data = yf.download(stock, start=start_date, end=end_date)

        # Iterate over the downloaded data and insert into the table
        for index, row in data.iterrows():
            sql = """
            INSERT INTO stock_prices (symbol, date, open, high, low, close, volume)
            VALUES ('{}', '{}', {}, {}, {}, {}, {});
            """.format(stock, index.date(), row['Open'], row['High'], row['Low'], row['Close'], row['Volume'])
            task = PostgresOperator(
                task_id='insert_stock_prices_{}_{}'.format(stock, index.date()),
                sql=sql,
                postgres_conn_id='your_postgres_connection_id',
                dag=dag
            )
            task.execute()

# Define the DAG
dag = DAG(
    'realtime_equity_pricing_dag',
    description='Download real-time stock pricing data from Yahoo Finance and store in PostgreSQL',
    schedule_interval='*/5 * * * *',
    start_date=datetime(2023, 5, 18),
    catchup=False
)

# Define the task
download_task = PythonOperator(
    task_id='download_stock_prices',
    python_callable=download_stock_prices,
    dag=dag
)

# Set the task dependencies
download_task
