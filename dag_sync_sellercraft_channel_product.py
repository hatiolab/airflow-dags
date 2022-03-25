from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.models.variable import Variable
from wms.sync_all_market_place_channel_products import (
    sync_all_market_place_channel_products,
)

# define default arguments for dags
default_args = {
    "owner": "jinwon",
    "depends_on_past": False,
    "start_date": datetime(2022, 1, 1),
    "email": ["jinwon@ai-doop.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

# define a dag with a timedata-based schedule
with DAG(
    "dag_sync_sellercraft_channel_product_mutation",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    # python task functions
    def run() -> None:

        import jwt
        from gql import gql, Client as GqlClient
        from gql.transport.requests import RequestsHTTPTransport

        try:
            host_url = Variable.get("OPERATO_COREAPP_URL")
            access_token = Variable.get("OPERATO_COREAPP_ACCESS_TOKEN")

            result = sync_all_market_place_channel_products(host_url, access_token)
            print("mutation execution result: ", result)

        except Exception as ex:
            print("Exception: ", ex)
            raise ValueError("Exception: ", ex)

    task1 = PythonOperator(
        task_id="main",
        python_callable=run,
        dag=dag,
    )
