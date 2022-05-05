from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.models.variable import Variable
from wms.sync_all_sftp_orders import (
    sync_all_sftp_orders,
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
    "dag_sync_sftp_orders_mutation",
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
            customer_domain_id = Variable.get("DEV_2_SFTP_DOMAIN_ID")

            print("---------------------------------------------------------------")
            print("[Main] decoded_token: ", decoded_token)
            print("[Main] things_factory_domain: ", things_factory_domain)
            print("[Main] customer_domain_id: ", customer_domain_id)
            print("[Main] access_token: ", access_token)
            print("---------------------------------------------------------------")    

            result = sync_all_sftp_orders(host_url, access_token, customer_domain_id)
            print("mutation execution result: ", result)

        except Exception as ex:
            print("Exception: ", ex)
            raise ValueError("Exception: ", ex)

    task1 = PythonOperator(
        task_id="main",
        python_callable=run,
        dag=dag,
    )
