import unittest
from datetime import datetime
from unittest.mock import Mock, patch
from airflow import DAG
from airflow.models import Variable
from rekcurd_airflow.operators import ModelSwitchOperator


class TestModelSwitchOperator(unittest.TestCase):
    @staticmethod
    def setUpClass():
        Variable.set('rekcurd_access_token', 'my_token')

    @staticmethod
    def tearDownClass():
        Variable.set('rekcurd_access_token', None)

    def setUp(self):
        self.dag = DAG(dag_id='test_model_switch_operator', start_date=datetime.now())

    def __set_mock(self, http_hook_class_mock):
        self.http_hook_mock = Mock()
        http_hook_class_mock.return_value = self.http_hook_mock
        self.http_hook_mock.run.return_value = Mock(
            text='{"status": true, "message": "success"}')
        self.ti_mock = Mock()
        self.ti_mock.xcom_pull.return_value = 7

    def __execute(self, http_hook_class_mock,
                  model_id=None,
                  model_provide_task_id=None):
        self.__set_mock(http_hook_class_mock)

        task = ModelSwitchOperator(task_id='rekcurd_api',
                                   dag=self.dag,
                                   project_id=1, app_id='sample_app',
                                   service_id=2,
                                   model_id=model_id,
                                   model_provide_task_id=model_provide_task_id)

        context = {'ti': self.ti_mock}
        task.execute(context)

    @patch("rekcurd_airflow.operators.rekcurd_operator.HttpHook")
    def test_switch_model_by_model_id(self, http_hook_class_mock):
        self.__execute(http_hook_class_mock, model_id=3)

        http_hook_class_mock.assert_called_with('PUT', http_conn_id='rekcurd_dashboard')
        self.http_hook_mock.run.assert_called_with(
            '/api/applications/1/services/2',
            'model_id=3',
            {
                'Authorization': 'Bearer my_token',
                "Content-Type": "application/x-www-form-urlencoded"
            },
            {'timeout': 300})

    @patch("rekcurd_airflow.operators.rekcurd_operator.HttpHook")
    def test_switch_model_param_required(self, http_hook_class_mock):
        with self.assertRaises(ValueError) as cm:
            self.__execute(http_hook_class_mock)

        self.assertEqual(
            str(cm.exception),
            'Value must be assigned to either `model_id` or `model_provide_task_id`.')

    @patch("rekcurd_airflow.operators.rekcurd_operator.HttpHook")
    def test_ignore_model_provide_task_id(self, http_hook_class_mock):
        self.__execute(http_hook_class_mock, model_id=3, model_provide_task_id='push')

        http_hook_class_mock.assert_called_with('PUT', http_conn_id='rekcurd_dashboard')
        self.http_hook_mock.run.assert_called_with(
            '/api/applications/1/services/2',
            'model_id=3',
            {
                'Authorization': 'Bearer my_token',
                "Content-Type": "application/x-www-form-urlencoded"
            },
            {'timeout': 300})

    @patch("rekcurd_airflow.operators.rekcurd_operator.HttpHook")
    def test_switch_model_by_returned_value(self, http_hook_class_mock):
        self.__execute(http_hook_class_mock,
                       model_provide_task_id='push_by_return')

        http_hook_class_mock.assert_called_with('PUT', http_conn_id='rekcurd_dashboard')
        self.http_hook_mock.run.assert_called_with(
            '/api/applications/1/services/2',
            'model_id=7',
            {
                'Authorization': 'Bearer my_token',
                "Content-Type": "application/x-www-form-urlencoded"
            },
            {'timeout': 300})
        self.ti_mock.xcom_pull.assert_called_with(task_ids="push_by_return")
