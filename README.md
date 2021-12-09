# Airflow Dags

## Airflow Tutorials
https://fluffy-print-13d.notion.site/Concepts-c33359b09dcf4fc2ab20994d38b50bab

## Dag Code Structure
```python
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.models.variable import Variable
from airflow.models.connection import Connection

# TODO DEFINES DEFAULT ARGUMENTS of A DAG
default_args = {
    "owner": "TODO_OWNER",
    "depends_on_past": False,
    "start_date": datetime(2021, 11, 1),
    "email": ["TODO_EMAIL"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": 0,
}

# TODO DEFINES A DAG
with DAG(
    "TODO_DAG_NAME",
    default_args=default_args,
    schedule_interval=None,
) as dag:

    # TODO: CREATE YOUR TASKS
    def function_name_1():
        #TODO YOUR CODE

    t1 = PythonOperator(
        task_id="TODO_TASK_NAME_1",
        python_callable=...,
        dag=dag,
    )
    
    t2 = PythonOperator(
        task_id="TODO_TASK_NAME_2",
        python_callable=...,
        dag=dag,
    )
    
    # TODO MAKE DEPENDENCIES AMONG TASKS
    t1 >> t2
```

## Dag Samples
### dag_sample_db_psycopg2.py
- this sample code shows how to access postgres database
- use psycopg2 as db handler

### dag_sample_db_sqlalchemy.py
- this sample code shows how to access postgres database
- use sqlalchemy as db handler

### dag_sample_postgres_operator.py
- this sample code shows how to access postgres database
- use PostgresOperator as one of airflow operators

### dag_sample_envs.py
- this sample code shows how to use Connection & Variable in Airflow environment

### dag_sample_xcom.py
- this sample code shows how to communicate data among tasks using xcom module
- please refer to [xcom](https://airflow.apache.org/docs/apache-airflow/stable/concepts/xcoms.html)

### dag_sample_publish.py
- this sample code shows how to publish data based on grapgql pusub mechanism

## How to build your dependencies(PIP modules)
- Add your additional pip modules to **requirements.txt**. That's all.

## Where to find any other Operators and Sensors
- [Airflow Operators](https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/operators/index.html)
- [Airflow Sensors](https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/sensors/index.html)
- [3rd Party Packages](https://registry.astronomer.io/)

