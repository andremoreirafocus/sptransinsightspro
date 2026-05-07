## Purpose of this project

Enable exploratory data analysis with minimal infrastructure.

## What this project does

Provides a notebook environment using Jupyter with DuckDB preinstalled so that exploratory analysis can be performed on the trusted layer implemented with object storage.
Notebooks are created inside the `notebooks` folder.

## Prerequisites

Each subproject has its own specific prerequisites described in the corresponding subfolder.

## Configuration

Grant write permission to the `notebooks` subfolder used to store notebooks:

```bash
sudo chown -R 1000:100 ./jupyter/notebooks
chmod -R 777 ./jupyter/notebooks
```

## Execution instructions

Jupyter starts through Docker Compose when all services are started with:

```bash
docker compose up -d
```

If you want to start only this service:

```bash
docker compose up -d jupyter
```

## Accessing Jupyter

Open:

`http://localhost:8888`

The notebook [positions_for_line_and_vehicle_and_day.ipynb](./notebooks/positions_for_line_and_vehicle_and_day.ipynb) contains example functions for accessing DuckDB for exploratory analysis.
