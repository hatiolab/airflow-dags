from airflow import DAG
from airflow.operators.bash_operator import BashOperator

from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# define default arguments for dags
default_args = {
    "owner": "jinwon",
    "depends_on_past": False,
    "start_date": datetime(2021, 11, 1),
    "email": ["jinwon@ai-doop.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

# define a dag with a timedata-based schedule
with DAG(
    "sample_db_psycopg2",
    default_args=default_args,
    schedule_interval=None,
) as dag:

    # python callable task: insert new data to 'testdata' table using SQL
    def insert_new_data():
        import psycopg2 as pg2
        from airflow.models.connection import Connection

        import time
        import random

        try:
            connection = Connection.get_connection_from_secrets("postgres_default")
            print(
                connection.login,
                connection.password,
                connection.host,
                connection.port,
                connection.schema,
            )

            with pg2.connect(
                user=connection.login,
                password=connection.password,
                host=connection.host,
                port=connection.port,
                database=connection.schema,
            ) as conn:
                with conn.cursor() as cursor:
                    # check if the table exists
                    cursor.execute(
                        "select exists(select * from information_schema.tables where table_name=%s)",
                        ("testdata",),
                    )
                    existTable = cursor.fetchone()[0]
                    print("testdata table existence: ", existTable)

                    if not existTable:
                        cursor.execute(
                            """
                            CREATE TABLE testdata (
                                id SERIAL PRIMARY KEY,
                                date INTEGER NOT NULL,
                                name VARCHAR(255) NOT NULL,
                                count INTEGER NOT NULL
                            )
                            """
                        )

                    # insert data
                    inset_sql = (
                        "INSERT INTO testdata (date, name, count) VALUES (%s, %s, %s);"
                    )
                    cursor.execute(
                        inset_sql,
                        (
                            str(time.time()),
                            f"test_data_{random.randrange(0, 10000)}",
                            str(random.randrange(0, 100)),
                        ),
                    )
        except (Exception, pg2.Error) as error:
            print("Error while inserting data to db", error)
        finally:
            # closing database connection.
            if conn:
                conn.close()
                print("db connection is closed")

    t1 = PythonOperator(
        task_id="new_db_data",
        python_callable=insert_new_data,
        dag=dag,
    )

    # python callable task: query all data for 'testdata' table using SQL
    def query_all_data():
        import psycopg2 as pg2
        from airflow.models.connection import Connection

        try:
            connection = Connection.get_connection_from_secrets("postgres_default")
            with pg2.connect(
                user=connection.login,
                password=connection.password,
                host=connection.host,
                port=connection.port,
                database=connection.schema,
            ) as conn:
                with conn.cursor() as cursor:
                    # fetch all table data using select *
                    select_all_query = "select * from testdata"
                    cursor.execute(select_all_query)
                    testdata_records = cursor.fetchall()

                    print("Print each row and it's columns values")
                    for row in testdata_records:
                        print("id = ", row[0])
                        print("date = ", row[1])
                        print("name = ", row[2])
                        print("count = ", row[3])
                        print("")
        except (Exception, pg2.Error) as error:
            print("Error while fetching data from db", error)

        finally:
            # closing database connection.
            if conn:
                conn.close()
                print("db connection is closed")

    t2 = PythonOperator(
        task_id="query_test_data",
        python_callable=query_all_data,
        dag=dag,
    )

    t1 >> t2
