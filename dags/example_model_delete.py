"""
Example DAG where rekcurd_airflow plugins are used
"""
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from rekcurd_airflow.operators import ModelDeleteOperator
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

dag = DAG('example_model_delete', default_args=default_args, schedule_interval="@once")


def push_by_return(**kwargs):
    return 5


"""
In production environment, ModelUploadOperator will return new model's model_id
"""
push_by_return = PythonOperator(task_id='push_by_return', dag=dag,
                                python_callable=push_by_return)

# Delete Model whose ID = 3. (The model is in Application 1)
delete = ModelDeleteOperator(task_id='delete_op', project_id=1,
                             app_id='sample_app', model_id=3,
                             dag=dag)

# ModelDeleteOperator can receive model_id by using XCom.
# ModelDeleteOperator will receive the model ID returned by `model_provide_task`
# Delete Model whose ID = 5. (The model is in Application 1)
delete2 = ModelDeleteOperator(task_id='delete_op_xcom_return',
                              project_id=1, app_id='sample_app',
                              model_provide_task_id='push_by_return',
                              dag=dag)

delete2.set_upstream(push_by_return)
