from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import days_ago
from datetime import timedelta


def _check_evaluate_model(**context):
    eval_upstream = context["task_instance"].xcom_pull(
        dag_id="ml_train_and_deploy",
        task_ids="evaluate_model",
        key="return_value",
        include_prior_dates=True
    )
    print(f"Upstream result: {eval_upstream}")
    if eval_upstream == "deploy_model":
        return "wait_deployment"
    return "end"

def _generate_report(**context):
    model_id = context["task_instance"].xcom_pull(
        dag_id="ml_train_and_deploy",
        task_ids="train_model",
        key="model_id",
        include_prior_dates=True,
    )
    print(f"Generating report for model {model_id}")

with DAG(
    dag_id='ml_report',
    schedule_interval="10 * * * *",
    start_date=days_ago(2),
    catchup=False,
) as dag:
    checker = BranchPythonOperator(
        task_id="check_model_evaluation",
        python_callable=_check_evaluate_model,
        dag=dag,
    )

    end = DummyOperator(task_id="end", dag=dag)
    
    wait = ExternalTaskSensor(
        task_id="wait_deployment",
        external_dag_id="ml_train_and_deploy",
        external_task_id="deploy_model",
        execution_delta=timedelta(minutes=10),
        dag=dag,
    )

    report = PythonOperator(
        task_id="generate_report",
        python_callable=_generate_report,
        dag=dag,
    )

    checker >> end
    checker >> wait >> report
