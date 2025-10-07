from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from snowflake.connector.pandas_tools import write_pandas

from datetime import datetime
import pandas as pd
import yfinance as yf
import time, traceback
from airflow.datasets import Dataset
from airflow.operators.empty import EmptyOperator

SNOWFLAKE_CONN_ID = "snowflake_conn"
SF_DB  = Variable.get("sf_database",  default_var="USER_DB_JELLYFISH")
SF_SC  = Variable.get("sf_schema",    default_var="RAW")
SF_WH  = Variable.get("sf_warehouse", default_var="JELLYFISH_QUERY_WH")

RAW_PRICES_DS = Dataset(f"snowflake://{SF_DB}/{SF_SC}/RAW_PRICES")
try:
    TICKERS = Variable.get("STOCK_TICKERS", deserialize_json=True)
except Exception:
    TICKERS = ["AAPL", "GOOGL"]

CREATE_RAW_TABLE = f"""
CREATE TABLE IF NOT EXISTS {SF_DB}.{SF_SC}.RAW_PRICES (
  SYMBOL STRING NOT NULL,
  TRADE_DATE DATE NOT NULL,
  OPEN FLOAT, HIGH FLOAT, LOW FLOAT, CLOSE FLOAT, VOLUME FLOAT,
  LOAD_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT PK_RAW PRIMARY KEY (SYMBOL, TRADE_DATE)
);
"""

def fetch_one(symbol: str) -> pd.DataFrame:
    last_err = None
    for attempt in range(1, 4):
        try:
            d = yf.download(symbol, period="180d", interval="1d",
                            auto_adjust=False, threads=False, progress=False)
            if d is not None and not d.empty:
                df = d.reset_index().rename(columns={
                    "Date": "TRADE_DATE", "Open": "OPEN", "High": "HIGH",
                    "Low": "LOW", "Close": "CLOSE", "Volume": "VOLUME"
                })
                df["SYMBOL"] = symbol
                df["TRADE_DATE"] = pd.to_datetime(df["TRADE_DATE"], errors="coerce").dt.tz_localize(None).dt.date
                for c in ["OPEN","HIGH","LOW","CLOSE","VOLUME"]:
                    df[c] = pd.to_numeric(df[c], errors="coerce")
                return df[["SYMBOL","TRADE_DATE","OPEN","HIGH","LOW","CLOSE","VOLUME"]]
            last_err = ValueError("empty from yf.download")
        except Exception as e:
            last_err = e
        try:
            h = yf.Ticker(symbol).history(period="180d", interval="1d", auto_adjust=False)
            if h is not None and not h.empty:
                df = h.reset_index().rename(columns={
                    "Date":"TRADE_DATE","Open":"OPEN","High":"HIGH","Low":"LOW","Close":"CLOSE","Volume":"VOLUME"
                })
                df["SYMBOL"] = symbol
                df["TRADE_DATE"] = pd.to_datetime(df["TRADE_DATE"], errors="coerce").dt.tz_localize(None).dt.date
                for c in ["OPEN","HIGH","LOW","CLOSE","VOLUME"]:
                    df[c] = pd.to_numeric(df[c], errors="coerce")
                return df[["SYMBOL","TRADE_DATE","OPEN","HIGH","LOW","CLOSE","VOLUME"]]
        except Exception as e2:
            last_err = e2
        time.sleep(attempt)
    raise last_err or RuntimeError("yfinance error")

with DAG(
    dag_id="yfinance_snowflake_etl",
    description="Load 180d OHLCV from yfinance into Snowflake RAW_PRICES (transaction + dedup).",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["finance","yfinance","snowflake","etl"],
) as dag:

    @task
    def extract_transform(ticker: str) -> str:
        try:
            df = fetch_one(ticker)
        except Exception as e:
            print(f"❌ yfinance failed for {ticker}: {e}")
            traceback.print_exc()
            return None
        return df.to_json(orient="records", date_format="iso") if not df.empty else None

    @task
    def load_to_snowflake(data_json: str):
        if not data_json:
            print("⚠️ Nothing to load (upstream returned None).")
            return
        df = pd.read_json(data_json)

        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        conn = hook.get_conn()
        cur = conn.cursor()
        try:
            cur.execute(f"USE WAREHOUSE {SF_WH}")
            cur.execute(f"USE DATABASE {SF_DB}")
            cur.execute(f"USE SCHEMA {SF_SC}")

            cur.execute("BEGIN")
            cur.execute(CREATE_RAW_TABLE)

            try:
                success, nchunks, nrows, _ = write_pandas(
                    conn, df, table_name="RAW_PRICES",
                    database=SF_DB, schema=SF_SC, quote_identifiers=False
                )
                print(f"write_pandas rows={nrows}, chunks={nchunks}, success={success}")
            except Exception as e_wp:
                print("write_pandas failed; fallback to executemany:", e_wp)
                ins = f"""
                  INSERT INTO {SF_DB}.{SF_SC}.RAW_PRICES
                    (SYMBOL,TRADE_DATE,OPEN,HIGH,LOW,CLOSE,VOLUME)
                  VALUES (%(SYMBOL)s,%(TRADE_DATE)s,%(OPEN)s,%(HIGH)s,%(LOW)s,%(CLOSE)s,%(VOLUME)s)
                """
                cur.executemany(ins, df.to_dict("records"))

            cur.execute("CREATE OR REPLACE TEMP TABLE TMP AS SELECT * FROM RAW_PRICES")
            cur.execute("TRUNCATE TABLE RAW_PRICES")
            cur.execute("""
              INSERT INTO RAW_PRICES (SYMBOL, TRADE_DATE, OPEN, HIGH, LOW, CLOSE, VOLUME, LOAD_TS)
              SELECT SYMBOL, TRADE_DATE, OPEN, HIGH, LOW, CLOSE, VOLUME, LOAD_TS
              FROM TMP
              QUALIFY ROW_NUMBER() OVER (
                PARTITION BY SYMBOL, TRADE_DATE
                ORDER BY LOAD_TS DESC
              ) = 1
            """)

            cur.execute("COMMIT")
            print("✅ Commit: RAW_PRICES loaded & deduped.")
        except Exception:
            cur.execute("ROLLBACK")
            print("❌ Rollback due to error.")
            traceback.print_exc()
            raise
        finally:
            cur.close(); conn.close()

    extracted = extract_transform.expand(ticker=TICKERS)
    loads = load_to_snowflake.expand(data_json=extracted)

# Emit the dataset event once, after ALL tickers are loaded.
raw_prices_ready = EmptyOperator(
    task_id="raw_prices_ready",
    outlets=[RAW_PRICES_DS]
)
loads >> raw_prices_ready
