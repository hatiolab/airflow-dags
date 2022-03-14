from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.models.variable import Variable

from wms.mutations import sync_all_marketpalce_order


# define default arguments for dags
default_args = {
    "owner": "jinwon",
    "depends_on_past": False,
    "start_date": datetime(2022, 1, 1),
    "email": ["jinwon@ai-doop.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
}

# define a dag with a timedata-based schedule
with DAG(
    "dag_sync_marketplace_order_mutation2",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    # python task functions
    def run_application_mutation() -> None:
        host_url = Variable.get("OPERATO_COREAPP_URL")
        access_token = Variable.get("OPERATO_COREAPP_ACCESS_TOKEN")
        company_domain_id = Variable.get("COMPANY_DOMAIN_ID")

        sync_all_marketplace_order(host_url, access_token, company_domain_id)

    task1 = PythonOperator(
        task_id="run_application_mutation",
        python_callable=run_application_mutation,
        dag=dag,
    )
