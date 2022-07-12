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
    "sample_xcom",
    default_args=default_args,
    schedule_interval=None,
) as dag:

    # the return value of this function will be moved into next task.
    # focus in 'provide_context=True' specifiied in one of task parameters
    def test_1(**context):
        url = context["params"]["url"]
        return {
            "url": url,
        }

    t1 = PythonOperator(
        task_id="xcom_test_1",
        python_callable=test_1,
        params={"url": "https://example.com/hello"},
        provide_context=True,
        dag=dag,
    )

    # this task get the return value from the previous task(xcom_test_1).
    def test_2(**context):
        print(context)
        pull_result = context["task_instance"].xcom_pull(task_ids="xcom_test_1")
        print(pull_result)
        url = pull_result["url"] + "/post"
        return {
            "url": url,
        }

    t2 = PythonOperator(
        task_id="xcom_test_2",
        python_callable=test_2,
        provide_context=True,
        dag=dag,
    )

    send_slack_message = SlackWebhookOperator(
        task_id="send_slack",
        http_conn_id="slack_webhook",
        message="Hello slack",
        dag=dag,
    )

    t1 >> t2 >> send_slack_message
