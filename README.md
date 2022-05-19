# README

## Usage

1. Prepare your Python environment and database, in my case I'm using Python 3.9 and PostgreSQL.
2. Prepare the PostgreSQL instance with username `postgres`, password `postgres`, database name `airflow` on your local machine.
3. Adjust the `database.sql_alchemy_conn` inside `airflow.cfg.example` if needed.
4. Execute `install.sh` to install the dependencies and set up few Airflow configurations.
5. Execute `run.sh` to start the Airflow, you can then access it in `http://localhost:8080` at your browser and login using `admin:admin`.

## Unit Tests

Unit tests can be performed by executing `test.sh`.

## Notes

- To use the BigQuery operator, you need to set up the `google_cloud_default` connection.
