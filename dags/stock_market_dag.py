from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
import pendulum
import datetime
import requests
import logging


default_args = {
    "owner": "mo",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=10),
}


@dag(
    dag_id="stock_market_pipeline_massive",
    default_args=default_args,
    description="Fetch daily stock data for all US companies using Massive.com API",
    schedule=datetime.timedelta(days=1),
    start_date=pendulum.today("UTC").add(days=-30),
    catchup=True,
    tags=["stock", "massive_api", "airflow3"],
)
def stock_market_pipeline():
    @task
    def create_table_task():
        pg_hook = PostgresHook(postgres_conn_id="postgres_default")
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        create_table_sql = """
        CREATE TABLE IF NOT EXISTS stock_prices (
            symbol VARCHAR(10) NOT NULL,
            date DATE NOT NULL,
            open DECIMAL(10, 2),
            high DECIMAL(10, 2),
            low DECIMAL(10, 2),
            close DECIMAL(10, 2),
            volume BIGINT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (symbol, date)
        );
        """
        cursor.execute(create_table_sql)
        conn.commit()
        cursor.close()
        conn.close()

    @task
    def fetch_stock_data_task(**kwargs):
        api_key = Variable.get("massive_api_key", default=None)
        execution_date = kwargs["ds"]  # YYYY-MM-DD

        pg_hook = PostgresHook(postgres_conn_id="postgres_default")
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # 1. Get valid symbols from DB
        logging.info("Fetching valid tickers from database...")
        cursor.execute("SELECT symbol FROM tickers")
        valid_tickers = set(row[0] for row in cursor.fetchall())
        logging.info(f"Loaded {len(valid_tickers)} valid tickers from DB.")

        if not valid_tickers:
            logging.warning(
                "No tickers found in DB. Please run 'update_tickers_weekly' DAG first."
            )
            # We continue but the filter below will block everything, which is correct behavior if DB is empty.

        # 2. Fetch Daily Data for ALL symbols (Grouped Endpoint)
        url = f"https://api.massive.com/v2/aggs/grouped/locale/us/market/stocks/{execution_date}?adjusted=true&apiKey={api_key}"

        try:
            logging.info(f"Fetching grouped daily data for {execution_date}")
            response = requests.get(url)

            if response.status_code == 200:
                data = response.json()
                results = data.get("results", [])
                logging.info(f"Received {len(results)} records from API.")

                # Filter and Insert
                insert_values = []
                for row in results:
                    symbol = row.get("T")

                    # FILTER: Only insert if symbol is in our tickers table
                    if symbol not in valid_tickers:
                        continue

                    open_val = row.get("o")
                    high_val = row.get("h")
                    low_val = row.get("l")
                    close_val = row.get("c")
                    volume_val = row.get("v")

                    insert_values.append(
                        (
                            symbol,
                            execution_date,
                            open_val,
                            high_val,
                            low_val,
                            close_val,
                            volume_val,
                        )
                    )

                logging.info(
                    f"Inserting {len(insert_values)} filtered records into DB."
                )

                if insert_values:
                    insert_sql = """
                    INSERT INTO stock_prices (symbol, date, open, high, low, close, volume)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (symbol, date) DO UPDATE SET
                        open = EXCLUDED.open,
                        high = EXCLUDED.high,
                        low = EXCLUDED.low,
                        close = EXCLUDED.close,
                        volume = EXCLUDED.volume;
                    """
                    cursor.executemany(insert_sql, insert_values)
                    conn.commit()
            else:
                logging.error(
                    f"Failed to fetch grouped data: {response.status_code} - {response.text}"
                )

        except Exception as e:
            logging.error(f"Error processing data for {execution_date}: {e}")
            conn.rollback()

        cursor.close()
        conn.close()

    # Define dependencies
    create_table_task() >> fetch_stock_data_task()


# Instantiate the DAG
stock_market_pipeline()
