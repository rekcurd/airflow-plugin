"""
Example DAG where rekcurd_airflow plugins are used
"""
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from rekcurd_airflow.operators import ModelUploadOperator
from datetime import timedelta, datetime
import pickle


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


MODEL_PATH = '/tmp/tmp_rekcurd_model.pkl'
MODEL_DESCRIPTION = 'model_{}'.format(datetime.now().strftime('%m%d'))


def train_func(**kwargs):
    model = 'dummy model'
    kwargs['ti'].xcom_push(key=ModelUploadOperator.MODEL_KEY, value=model)
    kwargs['ti'].xcom_push(key=ModelUploadOperator.MODEL_DESCRIPTION_KEY, value='dummy')


def save_model_func(**kwargs):
    model = kwargs['ti'].xcom_pull(key=ModelUploadOperator.MODEL_KEY, task_ids='train')
    with open(MODEL_PATH, 'wb') as f:
        pickle.dump(model, f)


def get_print_model_id(task_id):
    def print_model_id(**kwargs):
        result = kwargs['ti'].xcom_pull(task_ids=task_id)
        print('model_id of uploaded data: ', result)
    return print_model_id


with DAG('example_model_upload', default_args=default_args, schedule_interval="@once") as dag:
    train = PythonOperator(task_id='train', python_callable=train_func, provide_context=True)
    save = PythonOperator(task_id='save', python_callable=save_model_func, provide_context=True)

    # upload saved model file.
    upload_file = ModelUploadOperator(task_id='upload_file',
                                      app_id=5,
                                      model_file_path=MODEL_PATH,
                                      model_description=MODEL_DESCRIPTION)
    # upload trained model data.
    upload_binary = ModelUploadOperator(task_id='upload_binary',
                                        app_id=5,
                                        model_provide_task_id='train',
                                        model_description=MODEL_DESCRIPTION)
    delete = BashOperator(task_id='delete',
                          bash_command='rm {}'.format(MODEL_PATH),
                          trigger_rule='all_done')

    print_id_file = PythonOperator(task_id='print_id_file',
                                   python_callable=get_print_model_id('upload_file'),
                                   provide_context=True)
    print_id_binary = PythonOperator(task_id='print_id_binary',
                                     python_callable=get_print_model_id('upload_binary'),
                                     provide_context=True)

    train >> save >> upload_file >> print_id_file >> delete
    train >> upload_binary >> print_id_binary
