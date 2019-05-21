"""
Example DAG where rekcurd_airflow plugins are used
"""
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from rekcurd_airflow.operators import ModelSwitchOperator
from datetime import timedelta


default_args = {
    'owner': 'rekcurd-airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
    'email': [],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
}

dag = DAG('example_model_switch', default_args=default_args, schedule_interval="@once")


def push_by_return(**kwargs):
    return 5


"""
In production environment, ModelUploadOperator will return new model's model_id
"""
push_by_return = PythonOperator(task_id='push_by_return', dag=dag, python_callable=push_by_return)

# Rekcurd service (ID = 2. Application ID of the service is 1) will use the model whose ID is 3
switch = ModelSwitchOperator(task_id='switch_op',
                             project_id=1,
                             app_id='sample_app',
                             service_id=2,
                             model_id=3,
                             dag=dag)

# ModelSwitchOperator will receive the value returned by `model_provide_task`
# In this case, the switched model ID will be 5.
switch2 = ModelSwitchOperator(task_id='switch_op_xcom_return',
                              project_id=1,
                              app_id='sample_app',
                              service_id=2,
                              model_provide_task_id='push_by_return',
                              dag=dag)

# Switch model_id to 3 -> 5
switch.set_upstream(push_by_return)
switch2.set_upstream(switch)
