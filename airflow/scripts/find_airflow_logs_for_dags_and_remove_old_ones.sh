DAG_NAME=transformlivedata-v10
find /opt/airflow/logs/dag_id=$DAG_NAME -type f -mtime +14
find /opt/airflow/logs/dag_id=$DAG_NAME -type f -mtime +14 -delete && find /opt/airflow/logs/dag_id=$DAG_NAME -type d -empty -delete