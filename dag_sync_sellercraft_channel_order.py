from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.models.variable import Variable
from wms.sync_all_market_place_channel_orders import (
    sync_all_market_place_channel_orders,
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
    "dag_sync_sellercraft_channel_order_mutation",
    default_args=default_args,
    schedule_interval=timedelta(minutes=3),
    catchup=False,
) as dag:

    # python task functions
    def run() -> None:

        import jwt
        from gql import gql, Client as GqlClient
        from gql.transport.requests import RequestsHTTPTransport

        try:
            # TODO: Fetch this information from Airflow Variables, so you need to set variables on airflow webserver
            host_url = Variable.get("OPERATO_COREAPP_URL")
            access_token = Variable.get("OPERATO_COREAPP_ACCESS_TOKEN")

            from_date = datetime.today() - timedelta(days=3)
            to_date = datetime.now().isoformat()

            sync_all_market_place_channel_orders(
                host_url, access_token, from_date, to_date
            )

        except Exception as ex:
            print("Exception: ", ex)
            raise ValueError("Exception: ", ex)

    task1 = PythonOperator(
        task_id="main",
        python_callable=run,
        dag=dag,
    )
