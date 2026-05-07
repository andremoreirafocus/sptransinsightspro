## Purpose of this project

Provide the development environment and source code for the DAGs used by Airflow in production.

## What this project does

Each DAG has its own subfolder containing the source code for its functions, whose contents, when replicated into the Airflow folder, enable the production implementation.
Configuration for each subproject is loaded through its configuration module, which identifies the execution environment, either production through Airflow or local development inside the corresponding subfolder within `dags-dev`.
Production implementation is done through the production Airflow DAGs in the `airflow` folder, with the creation of a file containing the DAG and all configurations.
Execution in the development environment is done through a Python script that performs all task logic in the same sequence as the corresponding production DAG in Airflow.

## Prerequisites

Each subproject has its own specific prerequisites described in its corresponding subfolder.

## Configuration

Each subproject has its own specific configuration described in the README inside its corresponding subfolder.

## Installation instructions

Python packages are managed in a unified way, the same way they are managed in Airflow, through a single `requirements.txt` file.

To install the requirements for all subprojects:
- `cd dags-dev`
- `python3 -m venv .venv`
- `source .venv/bin/activate`
- `pip install -r requirements.txt`

## Instructions for running scripts in development

Scripts are placed in the `dags-dev` folder following the same structure as the Airflow `dags` folder and should be run directly from that folder:

```bash
python <dag-name>-<dag-version>.py
```

If the `.env` file does not exist at the root of the project, create it with the variables listed as required in the README of each subproject.

## Unit tests

Each pipeline has a `tests/` folder with unit tests. Tests follow the dependency-injection pattern through optional parameters, without monkeypatching, and use shared fakes in [`tests/fakes/README-EN.md`](./tests/fakes/README-EN.md).

To run all tests from the `dags-dev` folder:

```bash
cd dags-dev
pytest
```

To run tests for a specific pipeline:

```bash
pytest <pipeline>/tests/
```

### Coverage (GTFS example)

To get coverage for the GTFS pipeline:

```bash
pytest gtfs/tests --cov=gtfs --cov-report=term-missing
```

Current GTFS status:
- 75 tests passing
- total coverage: 99%
