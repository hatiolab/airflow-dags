from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.models.variable import Variable
from airflow.models.connection import Connection

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
    "sample_envs",
    default_args=default_args,
    schedule_interval=None,
) as dag:

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

    t1 = PythonOperator(
        task_id="test_envs",
        python_callable=test_envs,
        dag=dag,
    )

    # using jinja template(Reference: https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html)
    t2 = BashOperator(
        task_id="var_with_jinja",
        bash_command="echo {{ var.value.POSTGRES_CORE_DATABASE }} / {{ var.value.POSTGRES_CORE_PORT }}",
        dag=dag,
    )

    t1 >> t2
