from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.models.variable import Variable

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
    "dag_sample_variable_tasks",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    def process_something(**kargs):
        print("domain_id: ", kargs["domain_id"])
        print("token: ", kargs["token"])

    process_task_list = list()

    # python task functions
    def prepare_tasks() -> None:
        # TODO: get domain id from external storage like database
        company_info_list = [
            {"domain_id": "aaaaa", "token": "12345"},
            {"domain_id": "bbbbb", "token": "67890"},
        ]

        for idx, val in enumerate(company_info_list):
            print(idx, val)
            process_task = PythonOperator(
                task_id=f"process_task_{idx}",
                python_callable=process_something,
                op_kwargs=val,
                dag=dag,
            )
            process_task_list.append(process_task)

        print(process_task)

    ready_task = PythonOperator(
        task_id="sync_data_preparation",
        python_callable=prepare_tasks,
        dag=dag,
    )

    ready_task >> process_task_list
