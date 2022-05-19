from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

dag_meta = [
    {
        'dag_id': 'dag_1',
        'tasks': [
            { 'task_id': 'task_1', 'upstream': None },
            { 'task_id': 'task_2', 'upstream': 'task_1' },
            { 'task_id': 'task_3', 'upstream': 'task_1' },
            { 'task_id': 'task_4', 'upstream': 'task_2' },
        ],
    },
    {
        'dag_id': 'dag_2',
        'tasks': [
            { 'task_id': 'task_1', 'upstream': None },
            { 'task_id': 'task_2', 'upstream': 'task_1' },
        ],
    },
]

for dag_config in dag_meta:
    dag_id = 'lvl2_dynamic_' + dag_config['dag_id']
    dag = DAG(dag_id=dag_id, schedule_interval=None, start_date=days_ago(2), catchup=False)
    globals()[dag_id] = dag

    task_meta = dag_config['tasks']

    task_bag = {}
    upstream_bag = {}

    # Prepare task
    for task_config in task_meta:
        task_id = task_config['task_id']
        upstream = task_config.get('upstream', None)

        task_bag[task_id] = DummyOperator(task_id=task_id, dag=dag)
        if upstream is not None:
            upstream_bag[task_id] = upstream

    # Set upstream
    for task_id, upstream in upstream_bag.items():
        task_bag[upstream].set_downstream(task_bag[task_id])
