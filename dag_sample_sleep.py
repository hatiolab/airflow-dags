import time
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.models.variable import Variable
from airflow.models.connection import Connection
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

# define default arguments for dags
default_args = {
    "owner": "jinwon",
    "depends_on_past": False,
    "start_date": datetime(2021, 11, 1),
    "email": ["jinwon@ai-doop.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=10),
}

# define a dag without any schedule, so this dag can be invoked by a user trigger only.
with DAG(
    "sleep_test",
    default_args=default_args,
    schedule_interval=None,
) as dag:

    # the return value of this function will be moved into next task.
    # focus in 'provide_context=True' specifiied in one of task parameters
    def test_1():

        time.sleep(90)

        return "sleep done.."

    t1 = PythonOperator(
        task_id="sleep_task_1",
        python_callable=test_1,
        provide_context=False,
        dag=dag,
    )
