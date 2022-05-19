from airflow import DAG
from airflow.exceptions import AirflowConfigException
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator

import logging


logger = logging.getLogger(__name__)

dag_meta = Variable.get('dynamic_dag', deserialize_json=True, default_var=[{}])

for dag_config in dag_meta:
    dag_id = 'lvl3b_dynamic_' + dag_config['dag_id']
    schedule_interval = dag_config.get('schedule_interval', None)

    dag = DAG(dag_id=dag_id, schedule_interval=schedule_interval, start_date=days_ago(2), catchup=False)
    globals()[dag_id] = dag

    task_meta = dag_config['tasks']

    task_bag = {}
    upstream_bag = {}

    # Prepare task
    for task_config in task_meta:
        task_id = task_config['task_id']
        upstream = task_config.get('upstream', None)
        task_type = task_config.get('type', 'dummy_operator')

        if task_type == 'dummy_operator':
            task_bag[task_id] = DummyOperator(task_id=task_id, dag=dag)
        elif task_type == 'bigquery_check_operator':
            sql = task_config.get('sql', 'SELECT 1')
            task_bag[task_id] = BigQueryCheckOperator(task_id=task_id, dag=dag, sql=sql, use_legacy_sql=False)
        else:
            raise AirflowConfigException(f"invalid task type: {task_type}")

        if upstream is not None:
            upstream_bag[task_id] = upstream

    # Set upstream
    for task_id, upstream in upstream_bag.items():
        for u in upstream.split(","):
            task_bag[u].set_downstream(task_bag[task_id])
