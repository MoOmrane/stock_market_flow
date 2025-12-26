from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
import requests
import logging
import time
from datetime import datetime, timedelta
import pendulum

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="update_tickers_weekly",
    default_args=default_args,
    description="Fetch all active US stock tickers and update the database weekly",
    schedule="@weekly",
    start_date=pendulum.today("UTC"),
    catchup=False,
)
def update_tickers_weekly():
    @task
    def update_tickers_task():
        api_key = Variable.get("massive_api_key", default_var="DEMO_KEY")
        pg_hook = PostgresHook(postgres_conn_id="postgres_default")
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # 1. Create tickers table
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS tickers (
            symbol VARCHAR(10) PRIMARY KEY,
            name VARCHAR(255),
            exchange VARCHAR(50),
            asset_type VARCHAR(50),
            ipo_date DATE,
            status VARCHAR(20),
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        cursor.execute(create_table_sql)
        conn.commit()

        # 2. Fetch Tickers from Massive.com (Paginated API)
        url = f"https://api.massive.com/v3/reference/tickers?market=stocks&active=true&limit=1000&apiKey={api_key}"
        tickers_count = 0

        # Full refresh strategy
        cursor.execute("TRUNCATE TABLE tickers;")
        conn.commit()

        while url:
            try:
                logging.info(f"Fetching tickers from: {url}")
                response = requests.get(url)
                if response.status_code != 200:
                    logging.error(
                        f"Failed to fetch tickers: {response.status_code} - {response.text}"
                    )
                    break

                data = response.json()

                if "results" in data:
                    values_list = []
                    for item in data["results"]:
                        symbol = item.get("ticker")
                        name = item.get("name")
                        exchange = item.get("primary_exchange")
                        asset_type = item.get("type")
                        ipo_date = item.get("list_date")

                        if not symbol:
                            continue

                        values_list.append(
                            (symbol, name, exchange, asset_type, ipo_date)
                        )

                    if values_list:
                        insert_sql = """
                        INSERT INTO tickers (symbol, name, exchange, asset_type, ipo_date, status)
                        VALUES (%s, %s, %s, %s, %s, 'Active')
                        ON CONFLICT (symbol) DO NOTHING;
                        """
                        cursor.executemany(insert_sql, values_list)
                        tickers_count += len(values_list)
                        conn.commit()

                if "next_url" in data:
                    url = f"{data['next_url']}&apiKey={api_key}"
                    time.sleep(12)
                else:
                    url = None

            except Exception as e:
                logging.error(f"Error fetching tickers: {e}")
                break

        logging.info(f"Successfully updated {tickers_count} tickers.")
        cursor.close()
        conn.close()

    # Execute the task
    update_tickers_task()


# Instantiate the DAG
update_tickers_weekly()
