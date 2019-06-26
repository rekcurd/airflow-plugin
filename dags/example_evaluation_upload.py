"""
Example DAG where rekcurd_airflow plugins are used
"""
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from rekcurd_airflow.operators import EvaluationUploadOperator
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

EVAL_PATH = '/tmp/tmp_rekcurd_eval.txt'


def write_eval_file(**kwargs):
    with open(EVAL_PATH, 'w') as f:
        f.write('1\t0\t2\t3')


def print_evaluation_id(**kwargs):
    result = kwargs['ti'].xcom_pull(task_ids='upload_file')
    print('evaluation_id of uploaded data: ', result)


with DAG('example_evaluation_upload', default_args=default_args, schedule_interval="@once") as dag:
    write = PythonOperator(task_id='write',
                           python_callable=write_eval_file,
                           provide_context=True)

    # Upload data file to be evaluated to Rekcurd
    upload_evaluation_file = EvaluationUploadOperator(
        task_id='upload_file', project_id=1, app_id='sample_app',
        evaluation_file_path=EVAL_PATH)

    print_id = PythonOperator(task_id='print',
                              python_callable=print_evaluation_id,
                              provide_context=True)

    delete = BashOperator(task_id='delete',
                          bash_command='rm {}'.format(EVAL_PATH),
                          trigger_rule='all_done')

    write >> upload_evaluation_file >> print_id >> delete
