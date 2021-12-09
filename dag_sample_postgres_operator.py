# [START postgres_operator_howto_guide]
import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

# create_pet_table, populate_pet_table, get_all_pets, and get_birth_date are examples of tasks created by
# instantiating the Postgres Operator

# define a dag with "@once", so this dag will be run at "start_date"
with DAG(
    dag_id="merry_christmas",
    start_date=datetime.datetime(2021, 12, 25),
    schedule_interval="@once",
    catchup=False,
) as dag:
    # [START postgres_operator_howto_guide_create_pet_table]
    create_pet_table = PostgresOperator(
        task_id="create_pet_table",
        postgres_conn_id="postgres_default",
        sql="""
            CREATE TABLE IF NOT EXISTS pet (
            pet_id SERIAL PRIMARY KEY,
            name VARCHAR NOT NULL,
            pet_type VARCHAR NOT NULL,
            birth_date DATE NOT NULL,
            OWNER VARCHAR NOT NULL);
          """,
        dag=dag,
    )
    # [END postgres_operator_howto_guide_create_pet_table]
    # [START postgres_operator_howto_guide_populate_pet_table]
    populate_pet_table = PostgresOperator(
        task_id="populate_pet_table",
        postgres_conn_id="postgres_default",
        sql="""
            INSERT INTO pet (name, pet_type, birth_date, OWNER)
            VALUES ( 'Max', 'Dog', '2018-07-05', 'Jane');
            INSERT INTO pet (name, pet_type, birth_date, OWNER)
            VALUES ( 'Susie', 'Cat', '2019-05-01', 'Phil');
            INSERT INTO pet (name, pet_type, birth_date, OWNER)
            VALUES ( 'Lester', 'Hamster', '2020-06-23', 'Lily');
            INSERT INTO pet (name, pet_type, birth_date, OWNER)
            VALUES ( 'Quincy', 'Parrot', '2013-08-11', 'Anne');
            """,
        dag=dag,
    )
    # [END postgres_operator_howto_guide_populate_pet_table]
    # [START postgres_operator_howto_guide_get_all_pets]
    get_all_pets = PostgresOperator(
        task_id="get_all_pets",
        postgres_conn_id="postgres_default",
        sql="SELECT * FROM pet;",
        dag=dag,
    )
    # [END postgres_operator_howto_guide_get_all_pets]
    # [START postgres_operator_howto_guide_get_birth_date]
    get_birth_date = PostgresOperator(
        task_id="get_birth_date",
        postgres_conn_id="postgres_default",
        sql="""
            SELECT * FROM pet
            WHERE birth_date
            BETWEEN SYMMETRIC DATE '{{ params.begin_date }}' AND DATE '{{ params.end_date }}';
            """,
        params={"begin_date": "2020-01-01", "end_date": "2020-12-31"},
        dag=dag,
    )
    # [START postgres_operator_howto_guide_get_birth_date]

    create_pet_table >> populate_pet_table >> get_all_pets >> get_birth_date
    # [END postgres_operator_howto_guide]
