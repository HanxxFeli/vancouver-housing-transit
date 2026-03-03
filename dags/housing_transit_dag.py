"""
housing_transit_dag.py

PURPOSE: Defines the Vancouver Housing-Transit pipeline as an Airflow DAG.

Pipeline structure:
  ingest_property_tax ──┐
                         ├──► transform_properties ──► transform_proximity ──► transform_gold
  ingest_translink ─────┘

  Runs the pipeline using Bash Operator since airflow cannot handle pyspark tasks.
  each task calls pipeline.py with specific stages

Schedule: Runs every sunday at 2am
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash  import BashOperator


# ─── Default Task Arguments ───────────────────────────────────────────────────
# These apply to all tasks in the DAG
default_args: dict[str, any] = {
    'owner': 'Hans',
    'depends_on_past': False,      # Don't wait for previous run
    'email_on_failure': False,      # Don't send emails on failure
    'email_on_retry': False,        # Don't send emails on retry
    'retries': 1,                    # Retry once if a task fails
    'retry_delay': timedelta(minutes=5),  # Wait 5 minutes before retrying
}

# ─── DAG Definition ───────────────────────────────────────────────────────────
dag: DAG = DAG(
    dag_id='vancouver_housing_pipeline',
    default_args=default_args,
    description='Bronze → Silver → Gold pipeline for Vancouver housing data',
    schedule='0 2 * * 0',          # Runs every 2am every Sunday
    start_date=datetime(2026, 1, 1),
    catchup=False,                    # Don't backfill missed runs
    tags=['vancouver', 'housing', 'pipeline'],
    doc_md=__doc__,                   # Use this file's docstring as DAG docs
)

# ─── Task Definitions ─────────────────────────────────────────────────────────
# Each task runs pipeline.py with a specific --stages flag
# The cd /opt/airflow ensures we're in the right directory

task_ingest_properties = BashOperator(
    task_id='ingest_property_tax',
    bash_command='cd /opt/airflow && python pipeline_src/pipeline.py --stages ingest_properties',
    dag=dag,
)

task_ingest_translink = BashOperator(
    task_id='ingest_translink',
    bash_command='cd /opt/airflow && python pipeline_src/pipeline.py --stages ingest_translink',
    dag=dag,
)

task_transform: BashOperator = BashOperator(
    task_id='transform_data',
    bash_command='cd /opt/airflow && python pipeline_src/pipeline.py --stages transform',
    dag=dag,
)

task_gold: BashOperator = BashOperator(
    task_id='gold_layer',
    bash_command='cd /opt/airflow && python pipeline_src/pipeline.py --stages gold',
    dag=dag,
)

# ─── Dependencies ─────────────────────────────────────────────────────────────
# Set the order: ingest must complete before transform,
# transform must complete before gold
[task_ingest_properties, task_ingest_translink] >> task_transform >> task_gold
