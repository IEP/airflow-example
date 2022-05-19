from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

with DAG(
    dag_id='basic',
    schedule_interval=None,
    start_date=days_ago(2),
    catchup=False,
) as dag:
    task_bag = {}

    # Prepare task
    task_bag['task_1'] = DummyOperator(
        task_id='task_1',
        dag=dag,
    )

    task_bag['task_2'] = DummyOperator(
        task_id='task_2',
        dag=dag,
    )

    task_bag['task_3'] = DummyOperator(
        task_id='task_3',
        dag=dag,
    )

    task_bag['task_4'] = DummyOperator(
        task_id='task_4',
        dag=dag,
    )

    task_bag['task_5'] = DummyOperator(
        task_id='task_5',
        dag=dag,
    )

    # Set dependency
    task_bag['task_1'].set_downstream(task_bag['task_3'])
    task_bag['task_2'].set_downstream(task_bag['task_3'])
    task_bag['task_3'].set_downstream(task_bag['task_4'])
