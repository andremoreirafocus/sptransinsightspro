# Use a lightweight Python image
FROM python:3.10-slim

# Prevent Python from writing .pyc files and enable unbuffered logging
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set the working directory to /app
WORKDIR /app

# 1. Copy only the requirements first to leverage Docker cache
# (Path is relative to the root of the repo where we run the build command)
# COPY extractlivedata/requirements.txt .
COPY ./requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# 2. Copy the rest of the application code
# We copy from the specific folder into the container's /app
# COPY extractlivedata/ .
COPY ./ .

# COPY extractlivedata/.env-docker ./.env 
COPY ./.env-docker ./.env 

# Set the entry point. 
# (Replace 'main.py' with your actual script name, e.g., 'extract.py')
CMD ["python", "main.py"]