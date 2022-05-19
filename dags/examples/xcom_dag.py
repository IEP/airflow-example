from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.dates import days_ago
import random
import uuid
import time


def _train_model(**context):
    model_id = str(uuid.uuid4())
    context["task_instance"].xcom_push(key="model_id", value=model_id)

def _evaluate_model(**context):
    model_id = context["task_instance"].xcom_pull(task_ids="train_model", key="model_id")
    print(f"Evaluating model {model_id}")

    auc_roc = random.random()
    print(f"AUC ROC score for model {model_id}: {auc_roc}")

    if auc_roc > 0.1:
        return "deploy_model"
    return "end"

def _deploy_model(**context):
    model_id = context["task_instance"].xcom_pull(task_ids="train_model", key="model_id")
    print(f"Deploying model {model_id}")
    time.sleep(60)
    print(f"Model deployed")


with DAG(
    dag_id="ml_train_and_deploy",
    schedule_interval="0 * * * *",
    start_date=days_ago(2),
    catchup=False,
) as dag:
    train_model = PythonOperator(
        task_id="train_model",
        python_callable=_train_model,
        dag=dag,
    )

    evaluate_model = BranchPythonOperator(
        task_id="evaluate_model",
        python_callable=_evaluate_model,
        dag=dag,
    )

    deploy_model = PythonOperator(
        task_id="deploy_model",
        python_callable=_deploy_model,
        dag=dag,
    )

    end = DummyOperator(task_id="end", dag=dag)

    train_model.set_downstream(evaluate_model)
    evaluate_model.set_downstream(deploy_model)
    evaluate_model.set_downstream(end)
