from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.models.variable import Variable
from wms.sync_avern_sellercraft_qty import (
    sync_avern_sellercraft_qty,
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
    "retry_delay": timedelta(minutes=1),
}

# define a dag with a timedata-based schedule
with DAG(
    "dag_sync_avern_sellercraft_qty_mutation",
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
            access_token = Variable.get("OPERATO_AVERN_ACCESS_TOKEN")
            sellercraft_id = Variable.get("OPERATO_AVERN_SELLERCRAFT_ID")
            fulfillment_center_id = Variable.get("OPERATO_AVERN_FULFILLMENT_CENTER_ID")

            sync_avern_sellercraft_qty(host_url, access_token, sellercraft_id, fulfillment_center_id)

        except Exception as ex:
            print("Exception: ", ex)
            raise ValueError("Exception: ", ex)

    task1 = PythonOperator(
        task_id="main",
        python_callable=run,
        dag=dag,
    )
