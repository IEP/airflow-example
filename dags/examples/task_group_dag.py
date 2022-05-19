from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

with DAG(
    dag_id='task_group',
    schedule_interval=None,
    start_date=days_ago(2),
    catchup=False,
) as dag:
    with TaskGroup("group1") as group1:
        task_1 = DummyOperator(task_id="task_1", dag=dag)
        task_2 = DummyOperator(task_id="task_2", dag=dag)
        task_3 = DummyOperator(task_id="task_3", dag=dag)

        task_1.set_downstream(task_3)
        task_2.set_downstream(task_3)

    task_4 = DummyOperator(task_id="task_4", dag=dag)
    
    # Option
    group1.set_downstream(task_4)
    # task3.set_downstream(task_4)
