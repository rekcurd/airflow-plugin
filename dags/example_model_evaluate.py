"""
Example DAG where rekcurd_airflow plugins are used
"""
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from rekcurd_airflow.operators import ModelEvaluateOperator
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


def push_by_return(**kwargs):
    return 5


def output_metrics(**kwargs):
    """
    Output should be like:
        accuracy: 0.78573
        precision: 0.79797 0.54825 0.79828
        recall: 0.79797 0.54825 0.79828
        fvalue: 0.79797 0.54825 0.79828
    """
    result = kwargs['ti'].xcom_pull(task_ids='evaluate_op')
    metrics = ['accuracy', 'precision', 'recall', 'fvalue']
    for m in metrics:
        if m == 'accuracy':
            print(m + ':', '{:.5f}'.format(result[m]))
        else:
            print(m + ':', ' '.join('{:.5f}'.format(r) for r in result[m]))


with DAG('example_model_evaluate', default_args=default_args, schedule_interval="@once") as dag:
    """
    In production environment, EvaluationUploadOperator will return new evaluation data's id
    """
    push_eval_id = PythonOperator(task_id='push_eval', python_callable=push_by_return)
    push_model_id = PythonOperator(task_id='push_model', python_callable=push_by_return)

    # Evaluate model whose ID is 3 (evaluation data ID is 5)
    evaluate = ModelEvaluateOperator(task_id='evaluate_op', app_id=1, model_id=3,
                                     evaluation_provide_task_id='push_eval')

    # Evaluate model whose ID is 5 (evaluation data ID is 2)
    evaluate2 = ModelEvaluateOperator(task_id='evaluate_op2', app_id=1, evaluation_id=2,
                                      model_provide_task_id='push_model')

    output_metrics = PythonOperator(task_id='output_metrics',
                                    python_callable=output_metrics,
                                    provide_context=True)

    push_eval_id >> evaluate >> output_metrics
    push_model_id >> evaluate2
