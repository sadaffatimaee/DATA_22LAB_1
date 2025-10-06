from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import traceback

SNOWFLAKE_CONN_ID = "snowflake_conn"
SF_DB  = Variable.get("sf_database",  default_var="USER_DB_JELLYFISH")
SF_SC  = Variable.get("sf_schema",    default_var="RAW")
SF_WH  = Variable.get("sf_warehouse", default_var="JELLYFISH_QUERY_WH")

with DAG(
    dag_id="TrainPredict",
    description="Snowflake ML: create model, forecast 7 days, union into FINAL_PRICES.",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["snowflake","ml","elt"],
) as dag:

    @task
    def train_and_predict():
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        conn = hook.get_conn(); cur = conn.cursor()
        try:
            cur.execute(f"USE WAREHOUSE {SF_WH}")
            cur.execute(f"USE DATABASE {SF_DB}")
            cur.execute(f"USE SCHEMA {SF_SC}")

            # TRAIN
            cur.execute("BEGIN")
            cur.execute("""
              CREATE OR REPLACE VIEW TRAIN_VIEW AS
              SELECT TRADE_DATE AS DATE, CLOSE, SYMBOL
              FROM RAW_PRICES
            """)
            cur.execute("""
              CREATE OR REPLACE SNOWFLAKE.ML.FORECAST PREDICT_STOCK_PRICE (
                INPUT_DATA        => SYSTEM$REFERENCE('VIEW','TRAIN_VIEW'),
                SERIES_COLNAME    => 'SYMBOL',
                TIMESTAMP_COLNAME => 'DATE',
                TARGET_COLNAME    => 'CLOSE',
                CONFIG_OBJECT     => {'ON_ERROR':'SKIP'}
              )
            """)
            cur.execute("COMMIT")

            # FORECAST
            cur.execute("BEGIN")
            cur.execute("""
              CALL PREDICT_STOCK_PRICE!FORECAST(
                FORECASTING_PERIODS => 7,
                CONFIG_OBJECT => {'prediction_interval': 0.90}
              )
            """)
            qid = cur.sfqid
            cur.execute(f"""
              CREATE OR REPLACE TABLE FORECAST_7D AS
              SELECT REPLACE(series,'"','') AS SYMBOL,
                     ts::DATE               AS FORECAST_DATE,
                     forecast               AS YHAT,
                     lower_bound            AS YHAT_LOWER,
                     upper_bound            AS YHAT_UPPER,
                     CURRENT_TIMESTAMP()    AS CREATED_AT
              FROM TABLE(RESULT_SCAN('{qid}'))
            """)
            cur.execute("COMMIT")

            # UNION
            cur.execute("BEGIN")
            cur.execute("""
              CREATE OR REPLACE TABLE FINAL_PRICES AS
              SELECT SYMBOL, TRADE_DATE AS PRICE_DATE, CLOSE AS PRICE, 'ACTUAL' AS SOURCE
              FROM RAW_PRICES
              UNION ALL
              SELECT f.SYMBOL, f.FORECAST_DATE AS PRICE_DATE, f.YHAT AS PRICE, 'FORECAST' AS SOURCE
              FROM FORECAST_7D f
              LEFT JOIN RAW_PRICES r ON r.SYMBOL=f.SYMBOL AND r.TRADE_DATE=f.FORECAST_DATE
              WHERE r.SYMBOL IS NULL
            """)
            cur.execute("COMMIT")
        except Exception:
            cur.execute("ROLLBACK"); traceback.print_exc(); raise
        finally:
            cur.close(); conn.close()

    train_and_predict()
