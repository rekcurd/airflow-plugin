import unittest
import json
from datetime import datetime
from unittest.mock import Mock, patch
from airflow import DAG
from airflow.models import Variable
from rekcurd_airflow.operators import ModelEvaluateOperator


class TestModelEvaluateOperator(unittest.TestCase):
    @staticmethod
    def setUpClass():
        Variable.set('rekcurd_access_token', 'my_token')

    @staticmethod
    def tearDownClass():
        Variable.set('rekcurd_access_token', None)

    def setUp(self):
        self.dag = DAG(dag_id='test_model_evaluate_operator', start_date=datetime.now())

    def __set_mock(self, http_hook_class_mock):
        self.http_hook_mock = Mock()
        http_hook_class_mock.return_value = self.http_hook_mock
        self._response = {"num": 10, "accuracy": 0.9, "fvalue": [0.8, 0.7],
                          "precision": [0.6, 0.5], "recall": [0.4, 0.3],
                          "option": {}, "status": True, "result_id": 1}
        self.http_hook_mock.run.return_value = Mock(
            text=json.dumps(self._response))
        self.ti_mock = Mock()
        self.ti_mock.xcom_pull.return_value = 7

    def __execute(self, http_hook_class_mock,
                  model_id=None,
                  model_provide_task_id=None,
                  evaluation_id=None,
                  evaluation_provide_task_id=None,
                  overwrite=False):
        self.__set_mock(http_hook_class_mock)

        task = ModelEvaluateOperator(task_id='rekcurd_api',
                                     dag=self.dag,
                                     app_id=1,
                                     model_id=model_id,
                                     overwrite=overwrite,
                                     model_provide_task_id=model_provide_task_id,
                                     evaluation_id=evaluation_id,
                                     evaluation_provide_task_id=evaluation_provide_task_id)

        context = {'ti': self.ti_mock}
        task.execute(context)

    @patch("rekcurd_airflow.operators.rekcurd_operator.HttpHook")
    def test_evaluate_by_passing_id(self, http_hook_class_mock):
        self.__execute(http_hook_class_mock, model_id=3, evaluation_id=5)

        http_hook_class_mock.assert_called_with('POST', http_conn_id='rekcurd_dashboard')
        self.http_hook_mock.run.assert_called_with(
            '/api/applications/1/evaluate',
            'model_id=3&evaluation_id=5&overwrite=false',
            {
                'Authorization': 'Bearer my_token',
                "Content-Type": "application/x-www-form-urlencoded"
            },
            {'timeout': 300})

    @patch("rekcurd_airflow.operators.rekcurd_operator.HttpHook")
    def test_evaluate_model_param_required(self, http_hook_class_mock):
        with self.assertRaises(ValueError) as cm:
            self.__execute(http_hook_class_mock, evaluation_id=5)

        self.assertEqual(
            str(cm.exception),
            'Value must be assigned to either `model_id` or `model_provide_task_id`.')

    @patch("rekcurd_airflow.operators.rekcurd_operator.HttpHook")
    def test_evaluate_evaluattion_param_required(self, http_hook_class_mock):
        with self.assertRaises(ValueError) as cm:
            self.__execute(http_hook_class_mock, model_id=5)

        self.assertEqual(
            str(cm.exception),
            'Value must be assigned to either `evaluation_id` or `evaluation_provide_task_id`.')

    @patch("rekcurd_airflow.operators.rekcurd_operator.HttpHook")
    def test_evaluate_by_returning_value_and_overwrite(self, http_hook_class_mock):
        self.__execute(http_hook_class_mock,
                       model_provide_task_id='push_by_return',
                       evaluation_provide_task_id='push_by_return',
                       overwrite=True)

        http_hook_class_mock.assert_called_with('POST', http_conn_id='rekcurd_dashboard')
        self.http_hook_mock.run.assert_called_with(
            '/api/applications/1/evaluate',
            'model_id=7&evaluation_id=7&overwrite=true',
            {
                'Authorization': 'Bearer my_token',
                "Content-Type": "application/x-www-form-urlencoded"
            },
            {'timeout': 300})
        self.ti_mock.xcom_pull.assert_called_with(task_ids="push_by_return")
