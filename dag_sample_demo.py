from datetime import datetime, timedelta
from airflow.models.variable import Variable
from airflow.models.connection import Connection

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
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
    "sample_dags_demo",
    default_args=default_args,
    schedule_interval=timedelta(minutes=2),
    catchup=False,
    dagrun_timeout=timedelta(minutes=30),
) as dag:

    # the return value of this function will be moved into next task.
    # focus in 'provide_context=True' specifiied in one of task parameters
    def test_1(**context):
        url = context["params"]["url"]
        return {
            "url": url,
        }

    xcom_test_1 = PythonOperator(
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

    xcom_test_2 = PythonOperator(
        task_id="xcom_test_2",
        python_callable=test_2,
        provide_context=True,
        dag=dag,
    )

    send_slack_message = SlackWebhookOperator(
        task_id="send_slack",
        http_conn_id="slack_webhook",
        message="Demo Dag is done...",
        dag=dag,
    )

    # python callble task: using connection & variables
    def test_envs():
        try:
            # get a connection info for "postgres_default"
            connection = Connection.get_connection_from_secrets("postgres_default")
            print("connection: ", connection.get_uri())

            print("Variable set")
            Variable.set("TESTKEY", "ABCD!@#$")
            print(Variable.get("TESTKEY"))

            print(Variable.get("POSTGRES_CORE_DATABASE"))
            print(Variable.get("POSTGRES_CORE_HOST"))
            print(Variable.get("POSTGRES_CORE_PASSWORD"))
            print(Variable.get("POSTGRES_CORE_PORT"))
            print(Variable.get("POSTGRES_CORE_USER"))
        except Exception as ex:
            print("Excpetion: ", ex)

    test_envs_1 = PythonOperator(
        task_id="test_envs",
        python_callable=test_envs,
        dag=dag,
    )

    # using jinja template(Reference: https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html)
    bash_with_jinja = BashOperator(
        task_id="var_with_jinja",
        bash_command="echo {{ var.value.POSTGRES_CORE_DATABASE }} / {{ var.value.POSTGRES_CORE_PORT }}",
        dag=dag,
    )

    print_info = BashOperator(
        task_id="print_info",
        bash_command="echo {{ ds }}, run_id={{ run_id }} | dag_run={{ dag_run }}",
        dag=dag,
    )

    (
        xcom_test_1
        >> xcom_test_1
        >> [test_envs_1, bash_with_jinja]
        >> print_info
        >> send_slack_message
    )
