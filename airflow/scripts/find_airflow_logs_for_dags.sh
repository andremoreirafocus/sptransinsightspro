DAG_NAME=transformlivedata-v10
find /opt/airflow/logs -path *$DAG_NAME* -type f | wc -l