# FROM quay.io/jupyter/scipy-notebook:latest

# # Install DuckDB and Boto3 (for S3/MinIO management)
# RUN pip install --no-cache-dir duckdb boto3

# # Pre-install DuckDB extensions so they are ready offline
# RUN duckdb -c "INSTALL httpfs; INSTALL aws;"

# # Set the working directory
# WORKDIR /home/jovyan/work


FROM quay.io/jupyter/scipy-notebook:latest

# Install DuckDB and Boto3
RUN pip install --no-cache-dir duckdb boto3

# Pre-install DuckDB extensions using Python
# This avoids the "command not found" error
RUN python3 -c "import duckdb; con = duckdb.connect(); con.execute('INSTALL httpfs; INSTALL aws;')"

# Set the working directory
WORKDIR /home/jovyan/work