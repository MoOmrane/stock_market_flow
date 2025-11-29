# Stock Market Data Pipeline

A robust, containerized data pipeline orchestrated with **Apache Airflow 3** to ingest, process, and store daily US stock market data.

## 🚀 Features
- **Dual-DAG Architecture**:
  - `update_tickers_weekly`: Fetches active US tickers from Massive.com.
  - `stock_market_pipeline`: Daily ETL that fetches OHLCV data for valid tickers.
- **Containerized**: Fully Dockerized setup with Airflow (Webserver, Scheduler) and PostgreSQL.
- **Efficient**: Uses `executemany` for bulk database inserts and `ON CONFLICT` for idempotency.
- **Modern Airflow**: Built using the TaskFlow API (`@task`, `@dag`) and Airflow 2.9+ / 3.0 standards.

## 🛠️ Tech Stack
- **Orchestration**: Apache Airflow 3
- **Language**: Python 3.8+
- **Database**: PostgreSQL
- **Infrastructure**: Docker & Docker Compose
- **API**: Massive.com (Stock Data)

## 📦 Setup & Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/yourusername/stock_market_flow.git
   cd stock_market_flow
   ```

2. **Configure Environment**
   Copy the example environment file and add your API Key:
   ```bash
   cp .env.example .env
   ```
   *Edit `.env` and add your `MASSIVE_API_KEY`.*

3. **Start the Services**
   ```bash
   docker-compose up -d
   ```

4. **Access Airflow**
   - URL: `http://localhost:8080`
   - User: `admin`
   - Password: `admin`

## 🏃‍♂️ Running the Pipeline
1. Activate the **`update_tickers_weekly`** DAG first to populate the `tickers` table.
2. Activate the **`stock_market_pipeline_massive`** DAG to start fetching daily data.

## 📂 Project Structure
```
.
├── dags/                   # Airflow DAG definitions
│   ├── stock_market_dag.py
│   └── update_tickers_dag.py
├── docker-compose.yaml     # Service orchestration
├── Dockerfile              # Custom Airflow image
├── requirements.txt        # Python dependencies
└── .env.example            # Environment config template
```
