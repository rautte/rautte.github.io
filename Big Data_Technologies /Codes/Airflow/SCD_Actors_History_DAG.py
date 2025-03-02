from airflow.decorators import dag
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import datetime, timedelta
from include.eczachly.poke_tabular_partition import poke_tabular_partition
from include.eczachly.trino_queries import run_trino_query_dq_check, execute_trino_query

import os
from airflow.models import Variable
local_script_path = os.path.join("include", 'eczachly/scripts/kafka_read_example.py')
tabular_credential = Variable.get("TABULAR_CREDENTIAL")

@dag(
    description="A dag that incrementally inserts data into an SCD type table in Icerberg",
    default_args={
        "owner": "Tejas Raut",
        "start_date": datetime(1914, 12, 28),
        "retries": 1,
        "execution_timeout": timedelta(hours=1),
    },
    start_date=datetime(1914, 12, 28),
    max_active_runs=1,
    schedule_interval="@yearly",
    catchup=True,
    template_searchpath='include/eczachly',
    tags=["community"],
)
def rautte_actors_history_scd_dag():

    current_year = '{{ ds }}'

    # Step 1: Waits for upstream partition to be ready
    upstream_table = 'bootcamp.actor_films'
    wait_for_web_events = PythonOperator(
        task_id='wait_for_web_events',
        python_callable=poke_tabular_partition,
        op_kwargs={
            "tabular_credential": tabular_credential,
            "table": upstream_table,
            "partition": "year = YEAR(DATE('{current_year}'))"
        },
        provide_context=True  # This allows you to pass additional context to the function
    )

    # Step 2: Creates the SCD table if not exists
    production_table = 'rauttejas.actors_history_scd'
    create_scd_step = PythonOperator(
        task_id="create_scd_step",
        depends_on_past=True,
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""

            CREATE OR REPLACE TABLE {production_table} (
                actor_id VARCHAR,
                actor_name VARCHAR,
                is_active BOOLEAN,
                start_year INTEGER,
                end_year INTEGER,
                current_year INTEGER
            )
            WITH 
            (
                FORMAT = 'PARQUET',
                partitioning = ARRAY['current_year']
            )

             """
        }
    )

    # Step 3: Creates the SCD table if not exists
    staging_table = 'rauttejas.actors_history_stg'
    create_stg_step = PythonOperator(
        task_id="create_stg_step",
        depends_on_past=True,
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""

            CREATE OR REPLACE TABLE {staging_table} (
                actor_id VARCHAR,
                actor VARCHAR,
                year INTEGER
            )

             """
        }
    )

    # Step 4: Creates the SCD table if not exists
    clear_prod_step = PythonOperator(
        task_id="clear_prod_step",
        depends_on_past=True,
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""

            DELETE FROM {production_table} 
            WHERE current_year = YEAR(DATE('{current_year}'))

             """
        }
    )

    # Step 5: Creates the SCD table if not exists
    clear_stg_step = PythonOperator(
        task_id="clear_stg_step",
        depends_on_past=True,
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""

            DELETE FROM {staging_table}
            WHERE current_year = YEAR(DATE('{current_year}'))

             """
        }
    )

    # Step 6: Creates the SCD table if not exists
    load_stg_step = PythonOperator(
        task_id="load_stg_step",
        depends_on_past=True,
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""

            SELECT 
                CASE
                    WHEN u.actor_id IS NULL THEN p.actor_id 
                    ELSE u.actor_id
                END AS actor_id,
                CASE
                    WHEN u.actor IS NULL THEN p.actor_name 
                    ELSE u.actor
                END AS actor,
                year
            FROM {upstream_table} u LEFT JOIN {production_table} p
                    ON u.actor_id = p.actor_id OR u.actor = p.actor_name
            WHERE u.year = YEAR(DATE('{current_year}'))
            GROUP BY 1,2,3

             """
        }
    )

    # # Optional: Truncates all data from Production Table
    # truncate_step = PythonOperator(
    #     task_id="truncate_step",
    #     depends_on_past=True,
    #     python_callable=execute_trino_query,
    #     op_kwargs={
    #         'query': f"""

    #         DELETE FROM {production_table}

    #          """
    #     }
    # )

    # Step 7: Pre-checks data quality for upstream table (DQ check 1)
    pre_dq_check = PythonOperator(
        task_id="pre_dq_check",
        depends_on_past=True,
        python_callable=run_trino_query_dq_check,
        op_kwargs={
            'query': f"""

            SELECT 
                CASE WHEN COUNT(1) > 0 THEN 1 ELSE 0 END AS data_exists
            FROM {staging_table}
            WHERE year = YEAR(DATE('{current_year}'))

            """
        }
    )

    # Step 8: Runs SCD logic   
    scd_step = PythonOperator(
        task_id="scd_step",
        depends_on_past=True,
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""

            INSERT INTO {production_table}
            WITH yesterday AS (
                SELECT *
                FROM {production_table}
                WHERE current_year = YEAR(DATE('{current_year}')) - 1
            ),
            today AS (
                SELECT 
                    actor_id,
                    actor,
                    year
                FROM {staging_table}
                WHERE year = YEAR(DATE('{current_year}'))
            ),
            checker AS (
                SELECT *
                FROM {production_table}
                WHERE current_year = YEAR(DATE('{current_year}'))
            ),
            changes AS (
                SELECT 
                    CASE
                        WHEN t.actor_id IS NULL THEN y.actor_id
                        WHEN y.actor_id IS NULL THEN t.actor_id
                        ELSE y.actor_id 
                    END AS actor_id,
                    CASE
                        WHEN t.actor_id IS NULL THEN y.actor_name
                        WHEN y.actor_id IS NULL THEN t.actor
                        ELSE y.actor_name
                    END AS actor_name,
                    CASE
                        WHEN t.actor_id IS NULL THEN False
                        WHEN y.actor_id IS NULL THEN True
                        ELSE True
                    END AS is_active,
                    CASE
                        WHEN y.is_active = False AND t.actor_id IS NULL 
                            THEN y.start_year
                        WHEN y.actor_id IS NULL THEN YEAR(DATE('{current_year}'))
                        WHEN y.is_active = True AND t.actor_id IS NOT NULL 
                            THEN y.start_year
                        ELSE YEAR(DATE('{current_year}'))
                    END AS start_year,
                    YEAR(DATE('{current_year}')) AS end_year,
                    YEAR(DATE('{current_year}')) AS current_year
                FROM yesterday y FULL OUTER JOIN today t
                    ON y.actor_id = t.actor_id
            ),
            non_duplicates AS (
                SELECT 
                    c.actor_id,
                    c.actor_name,
                    c.is_active,
                    c.start_year,
                    c.end_year,
                    c.current_year 
                FROM changes c LEFT JOIN checker prod
                    ON c.actor_id = prod.actor_id 
                        AND c.is_active = prod.is_active
                        AND c.start_year = prod.start_year
                        AND c.end_year = prod.end_year
                WHERE
                        prod.actor_id IS NULL
            )

            SELECT *
            FROM non_duplicates

            """        
        }
    )

    # Step 9: Post-checks data quality for production table (DQ check 2)
    post_dq_check = PythonOperator(
        task_id="post_dq_check",
        depends_on_past=True,
        python_callable=run_trino_query_dq_check,
        op_kwargs={
            'query': f"""

            SELECT 
                CASE WHEN COUNT(1) = COUNT(DISTINCT actor_id, start_year, end_year) THEN 1 ELSE 0 END AS no_duplicates,
                CASE WHEN COUNT(actor_id) > 0 THEN 1 ELSE 0 END AS valid_data
            FROM {production_table}
            WHERE current_year = YEAR(DATE('{current_year}'))

            """
        }
    )

    wait_for_web_events >> create_scd_step >> create_stg_step >> clear_prod_step >> clear_stg_step >> load_stg_step >> pre_dq_check >> scd_step >> post_dq_check
    


rautte_actors_history_scd_dag()
