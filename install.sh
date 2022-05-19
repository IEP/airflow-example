export AIRFLOW_HOME=$(pwd)

sed "s/AIRFLOW_HOME/${AIRFLOW_HOME//\//\\/}/g" airflow.cfg.example > airflow.cfg

AIRFLOW_VERSION=2.3.0
PYTHON_VERSION="$(python3 --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
pip install "apache-airflow[celery,google,postgres]==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

airflow db init
airflow users create --firstname admin --lastname admin --email admin --password admin --username admin --role Admin
airflow variables import files/dynamic_dag.json
