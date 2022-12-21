from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.email import EmailOperator

import requests
import json
from datetime import timedelta, datetime


default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "admin@localhost.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

def download_exchange_rates():
  BASE_URL = f"https://api.apilayer.com/exchangerates_data/latest?"
  END_POINTS = {
      "EUR": "symbols=USD,AUD,BRL,CAD,COP,DKK,EGP&base=EUR",
      "USD": "symbols=EUR,AUD,BRL,CAD,COP,DKK,EGP&base=USD",
      "BRL": "symbols=EUR,AUD,USD,CAD,COP,DKK,EGP&base=BRL"
  }
  headers= {
    "apikey": "your_API_key"
  }
  for i in range(len(END_POINTS)):
      base = list(END_POINTS)[i]
      indata = requests.get(f"{BASE_URL}{END_POINTS[base]}", headers=headers).json()
      outdata = {"base": base, "rates": indata["rates"], "last_update": indata["date"]}
      with open("/opt/airflow/dags/files/exchange_rates.json", "a") as outfile:
                  json.dump(outdata, outfile)
                  outfile.write("\n")


with DAG("exchange_rates_data_pipeline", start_date=datetime(2022, 1, 1), schedule_interval="@daily",
    default_args=default_args, catchup=False) as dag:

    is_exchange_rates_available = HttpSensor(
        task_id="is_exchange_rates_available",
        http_conn_id="exchange_rates_api",
        headers={"apikey": "your_API_key"},
        endpoint="exchangerates_data/latest",
        response_check=lambda response: "rates" in response.text,
        poke_interval=5,
        timeout=20
    )

    downloading_exchange_rates = PythonOperator(
        task_id="downloading_exchange_rates",
        python_callable=download_exchange_rates
    )

    saving_exchange_rates = BashOperator(
        task_id="saving_exchange_rates",
        bash_command="""
            hdfs dfs -mkdir -p /exchange_rates && \
            hdfs dfs -put -f $AIRFLOW_HOME/dags/files/exchange_rates.json /exchange_rates
        """
    )

    creating_exchange_rates_table = HiveOperator(
        task_id="creating_exchange_rates_table",
        hive_cli_conn_id="hive_conn",
        hql="""
            CREATE EXTERNAL TABLE IF NOT EXISTS exchange_rates(
                base STRING,
                last_update DATE,
                eur DOUBLE,
                usd DOUBLE,
                aud DOUBLE,
                brl DOUBLE,
                cad DOUBLE,
                cop DOUBLE,
                dkk DOUBLE,
                egp DOUBLE
                )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE
        """
    )

    exchange_rates_processing = SparkSubmitOperator(
        task_id="exchange_rates_processing",
        application="/opt/airflow/dags/scripts/exchange_rates_processing.py",
        conn_id="spark_conn",
        verbose=False
    )

    send_email_notification = EmailOperator(
        task_id="send_email_notification",
        to="david94zgz@gmail.com",
        subject="exchange_rate_data_pipeline",
        html_content=f"""
        <h2>exchange_rate_data_pipeline</h2>
        
        <h4>The exchange rate data pipeline has been succesfully executed on {datetime.today()}.</h4>
        """
    )


    is_exchange_rates_available >> downloading_exchange_rates >> saving_exchange_rates
    saving_exchange_rates >> creating_exchange_rates_table >> exchange_rates_processing
    exchange_rates_processing >> send_email_notification
