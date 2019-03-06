# Rekcurd-airflow
[![Build Status](https://travis-ci.com/rekcurd/airflow-plugin.svg?branch=master)](https://travis-ci.com/rekcurd/airflow-plugin)
[![PyPI version](https://badge.fury.io/py/rekcurd-airflow.svg)](https://badge.fury.io/py/rekcurd-airflow)
[![codecov](https://codecov.io/gh/rekcurd/airflow-plugin/branch/master/graph/badge.svg)](https://codecov.io/gh/rekcurd/airflow-plugin "Non-generated packages only")
[![pypi supported versions](https://img.shields.io/pypi/pyversions/rekcurd-airflow.svg)](https://pypi.python.org/pypi/rekcurd-airflow)

Airflow Plugins for [Rekcurd Dashboard](https://github.com/rekcurd/dashboard)

## Environment
- Python 3.6
- apache-airflow >= 1.10.x
- Rekcurd Dashboard >= 0.4.x

## Setup
- Setup Airflow
- Setup Rekcurd Dashboard and get your access token
- Run the following commands

**Use JWT token published by Rekcurd Dashboard as REKCURD_ACCESS_TOKEN for now**

```
# Set the access token and airflow-rekcurd connection to airflow
# Replace the environment variables with your own values.
$ cd $AIRFLOW_HOME
$ airflow variables -s rekcurd_access_token $REKCURD_ACCESS_TOKEN
$ airflow connections -a --conn_id rekcurd_dashboard \
	--conn_uri http://$REKCURD_DASHBOARD_HOST:$REKCURD_DASHBOARD_PORT/

$ pip install rekcurd-airflow
```

## Components
You can see example DAGs for the plugins in [here](./dags)

### Operators
- EvaluationUpdateOperator. ([Example DAG](./dags/example_evaluation_upload.py))
- ModelDeleteOperator. ([Example DAG](./dags/example_model_delete.py))
- ModelEvaluateOperator. ([Example DAG](./dags/example_model_evaluate.py))
- ModelSwitchOperator. ([Example DAG](./dags/example_model_switch.py))
- ModelUpdateOperator. ([Example DAG](./dags/example_model_upload.py))

[Overall Example DAG](./dags/example_all.py)

## Test

```
$ cd airflow-plugin # move to the root directory of this repo

# not necessary if you already setup airflow for this repository
$ export AIRFLOW_HOME=`pwd`
$ pip install -r test-requirements.txt
$ airflow initdb
$ fernet_key=`python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"`
$ sed -i -e "s/\(fernet_key =\).*/\1 $fernet_key/" airflow.cfg

$ python -m unittest
```
