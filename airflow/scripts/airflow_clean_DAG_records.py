python - <<EOF
from airflow.models import DagRun
from airflow.utils.session import create_session

# DAG_ID = 'refinedfinishedtrips-v6'
# DAG_ID = 'transformlivedata-v10'
DAG_ID = 'updatelatestpositions-v4'
DELETE_COUNT = 7000

with create_session() as session:
    # 1. Fetch ONLY the IDs of the oldest 7000 runs to avoid loading objects into memory
    runs_to_delete = session.query(DagRun.id)\
        .filter(DagRun.dag_id == DAG_ID)\
        .order_by(DagRun.execution_date.asc())\
        .limit(DELETE_COUNT)\
        .all()
    
    if not runs_to_delete:
        print(f"No records found for {DAG_ID}.")
    else:
        # Extract the raw IDs into a list
        run_ids = [run.id for run in runs_to_delete]
        
        # 2. Perform a bulk SQL delete. 
        # synchronize_session=False prevents SQLAlchemy from trying to evaluate the cascades in Python.
        deleted_count = session.query(DagRun)\
            .filter(DagRun.id.in_(run_ids))\
            .delete(synchronize_session=False)
        
        # 3. Commit the changes
        session.commit()
        print(f"Successfully deleted {deleted_count} oldest records for {DAG_ID}.")
EOF