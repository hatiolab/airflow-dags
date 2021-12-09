from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models.variable import Variable
from datetime import datetime, timedelta
import json
import redis

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
    "provide_context": True,
}

# define a dag without any schedule, so this dag can be invoked by a user trigger only.
with DAG(
    "sample_publish",
    default_args=default_args,
    schedule_interval=None,
) as dag:

    # create pubsub data
    def get_channgel_message(**context):
        from datetime import datetime

        time_data = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

        print(Variable)
        channel = "pubsubtest"
        message = f"this message from airflow - {time_data}"
        return {"channel": channel, "message": message}

    # publish data to server
    def publish_to_redis(**context):
        from airflow.models.connection import Connection

        # publish pubsub data to redis for the subscription of server
        key_message = context["ti"].xcom_pull(task_ids="get_channgel_message")

        # get redis connection information
        connection = Connection.get_connection_from_secrets("redis_default")

        try:
            rdis = redis.Redis(
                host=connection.host,
                port=connection.port,
                db=0,
            )
            channel = "data"
            message = {
                "data": {
                    "domain": {"subdomain": "system"},
                    "tag": key_message.get("channel") or "wrong_channel",
                    "data": key_message.get("message") or "wrong_message",
                }
            }
            rdis.publish(channel, json.dumps(message))
        except Exception as e:
            print("Exception: ", e)

    t1 = PythonOperator(
        task_id="get_channgel_message",
        python_callable=get_channgel_message,
        dag=dag,
    )

    t2 = PythonOperator(
        task_id="publish_to_redis",
        python_callable=publish_to_redis,
        provide_context=True,
        dag=dag,
    )

    t1 >> t2
