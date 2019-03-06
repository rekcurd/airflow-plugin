import unittest
from datetime import datetime
from unittest.mock import Mock, patch
from airflow import DAG
from airflow.models import Variable
from rekcurd_airflow.operators import ModelDeleteOperator


class TestModelDeleteOperator(unittest.TestCase):
    @staticmethod
    def setUpClass():
        Variable.set('rekcurd_access_token', 'my_token')

    @staticmethod
    def tearDownClass():
        Variable.set('rekcurd_access_token', None)

    def setUp(self):
        self.dag = DAG(dag_id='test_model_delete_operator', start_date=datetime.now())

    def __set_mock(self, http_hook_class_mock):
        self.http_hook_mock = Mock()
        http_hook_class_mock.return_value = self.http_hook_mock
        self.http_hook_mock.run.return_value = Mock(
            text='{"status": true, "message": "success"}')
        self.ti_mock = Mock()
        self.ti_mock.xcom_pull.return_value = 7

    def __execute(self, http_hook_class_mock,
                  model_id=None,
                  model_provide_task_id=None,
                  timeout=None):
        self.__set_mock(http_hook_class_mock)

        if timeout:
            task = ModelDeleteOperator(task_id='rekcurd_api',
                                       dag=self.dag,
                                       app_id=1,
                                       model_id=model_id,
                                       timeout=timeout,
                                       model_provide_task_id=model_provide_task_id)
        else:
            task = ModelDeleteOperator(task_id='rekcurd_api',
                                       dag=self.dag,
                                       app_id=1,
                                       model_id=model_id,
                                       model_provide_task_id=model_provide_task_id)

        context = {'ti': self.ti_mock}
        task.execute(context)

    @patch("rekcurd_airflow.operators.rekcurd_operator.HttpHook")
    def test_delete_model_by_model_id(self, http_hook_class_mock):
        self.__execute(http_hook_class_mock, model_id=3)

        http_hook_class_mock.assert_called_with('DELETE', http_conn_id='rekcurd_dashboard')
        self.http_hook_mock.run.assert_called_with(
            '/api/applications/1/models/3',
            {},
            {
                'Authorization': 'Bearer my_token'
            },
            {'timeout': 300})

    @patch("rekcurd_airflow.operators.rekcurd_operator.HttpHook")
    def test_delete_model_param_required(self, http_hook_class_mock):
        with self.assertRaises(ValueError) as cm:
            self.__execute(http_hook_class_mock)

        self.assertEqual(
            str(cm.exception),
            'Value must be assigned to either `model_id` or `model_provide_task_id`.')

    @patch("rekcurd_airflow.operators.rekcurd_operator.HttpHook")
    def test_ignore_model_provide_task_id(self, http_hook_class_mock):
        self.__execute(http_hook_class_mock, model_id=3, model_provide_task_id='push', timeout=150)

        http_hook_class_mock.assert_called_with('DELETE', http_conn_id='rekcurd_dashboard')
        self.http_hook_mock.run.assert_called_with(
            '/api/applications/1/models/3',
            {},
            {
                'Authorization': 'Bearer my_token'
            },
            {'timeout': 150})

    @patch("rekcurd_airflow.operators.rekcurd_operator.HttpHook")
    def test_delete_model_by_returned_value(self, http_hook_class_mock):
        self.__execute(http_hook_class_mock,
                       model_provide_task_id='push_by_return')

        http_hook_class_mock.assert_called_with('DELETE', http_conn_id='rekcurd_dashboard')
        self.http_hook_mock.run.assert_called_with(
            '/api/applications/1/models/7',
            {},
            {
                'Authorization': 'Bearer my_token',
            },
            {'timeout': 300})
        self.ti_mock.xcom_pull.assert_called_with(task_ids="push_by_return")
