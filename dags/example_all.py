"""
Example DAG where rekcurd_airflow plugins are used
"""
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash_operator import BashOperator
from rekcurd_airflow.operators import EvaluationUploadOperator, \
    ModelDeleteOperator, ModelEvaluateOperator, ModelSwitchOperator, ModelUploadOperator
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


def train_func(**kwargs):
    kwargs['ti'].xcom_push(key=ModelUploadOperator.MODEL_KEY,
                           value='dummy model content')
    kwargs['ti'].xcom_push(key=ModelUploadOperator.MODEL_DESCRIPTION_KEY,
                           value='dummy description')


def is_good_evaluation_result(**kwargs):
    result = kwargs['ti'].xcom_pull(task_ids='evaluate_model')
    threshold = 0.7
    if result['accuracy'] < threshold:
        return 'delete_model'
    else:
        return 'switch_development_model'


def output_metrics(**kwargs):
    """
    Output should be like:
        accuracy: 0.78573
        precision: 0.79797 0.54825 0.79828
        recall: 0.79797 0.54825 0.79828
        fvalue: 0.79797 0.54825 0.79828
    """
    result = kwargs['ti'].xcom_pull(task_ids='evaluate_model')
    metrics = ['accuracy', 'precision', 'recall', 'fvalue']
    for m in metrics:
        if m == 'accuracy':
            print(m + ':', '{:.5f}'.format(result[m]))
        else:
            print(m + ':', ' '.join('{:.5f}'.format(r) for r in result[m]))


with DAG('example_all', default_args=default_args, schedule_interval="@once") as dag:
    train = PythonOperator(task_id='train', python_callable=train_func, provide_context=True)

    application_id = 5
    sandbox_service_id = 10
    dev_service_id = 11

    upload_model = ModelUploadOperator(task_id='upload_model',
                                       app_id=application_id,
                                       model_provide_task_id='train')
    switch_sandbox_model = ModelSwitchOperator(task_id='switch_sandbox_model',
                                               app_id=application_id,
                                               service_id=sandbox_service_id,
                                               model_provide_task_id='upload_model')
    # wait until kubernetes cluster finishes rolling update.
    wait = BashOperator(task_id='wait_updating', bash_command='sleep 800')

    save_eval_file = PythonOperator(task_id='write_eval_file',
                                    python_callable=write_eval_file,
                                    provide_context=True)
    upload_evaluation_file = EvaluationUploadOperator(
        task_id='upload_eval_file', app_id=application_id, evaluation_file_path=EVAL_PATH)
    remove_eval_file = BashOperator(task_id='remove_local_eval_file',
                                    bash_command='rm {}'.format(EVAL_PATH),
                                    trigger_rule='all_done')

    evaluate_model = ModelEvaluateOperator(task_id='evaluate_model', app_id=application_id,
                                           model_provide_task_id='upload_model',
                                           evaluation_provide_task_id='upload_eval_file')

    output_metrics = PythonOperator(task_id='output_metrics',
                                    python_callable=output_metrics,
                                    provide_context=True)
    judge_metrics = BranchPythonOperator(task_id='judge_metrics',
                                         python_callable=is_good_evaluation_result,
                                         provide_context=True)
    delete_model = ModelDeleteOperator(task_id='delete_model',
                                       app_id=application_id,
                                       model_provide_task_id='upload_model')

    switch_dev_model = ModelSwitchOperator(task_id='switch_development_model',
                                           app_id=application_id,
                                           service_id=dev_service_id,
                                           model_provide_task_id='upload_model')

    """
    After completing switching model and uploading evaluation data, evaluate the new model.
    If the new model has low accuracy, delete the model.
    If the new model has high accuracy, use the model in development environment as well.
    """
    train >> upload_model >> switch_sandbox_model >> wait >> evaluate_model
    save_eval_file >> upload_evaluation_file >> remove_eval_file >> evaluate_model
    evaluate_model >> output_metrics >> judge_metrics >> switch_dev_model
    judge_metrics >> delete_model
