import unittest
from datetime import datetime
from unittest.mock import Mock, patch
from airflow import DAG
from airflow.models import Variable
from rekcurd_airflow.operators.rekcurd_operator import RekcurdOperator


class TestRekcurdOperator(unittest.TestCase):
    @staticmethod
    def setUpClass():
        Variable.set('rekcurd_access_token', 'my_token')

    @staticmethod
    def tearDownClass():
        Variable.set('rekcurd_access_token', None)

    def setUp(self):
        self.dag = DAG(dag_id='test_rekcurd_operator', start_date=datetime.now())

    @patch("rekcurd_airflow.operators.rekcurd_operator.HttpHook")
    def test_rekcurd_operator(self, http_hook_class_mock):
        http_hook_mock = Mock()
        http_hook_class_mock.return_value = http_hook_mock
        http_hook_mock.run.return_value = Mock(text='success')

        task = RekcurdOperator(task_id='rekcurd_api',
                               dag=self.dag,
                               timeout=100,
                               method='POST',
                               endpoint='api/test')

        context = None
        result = task.execute(context)

        self.assertEqual(result, 'success')
        http_hook_class_mock.assert_called_with('POST', http_conn_id='rekcurd_dashboard')
        http_hook_mock.run.assert_called_with(
            'api/test', {}, {'Authorization': 'Bearer my_token'}, {'timeout': 100}
        )

    @patch("rekcurd_airflow.operators.rekcurd_operator.HttpHook")
    def test_rekcurd_operator_with_extra_options(self, http_hook_class_mock):
        http_hook_mock = Mock()
        http_hook_class_mock.return_value = http_hook_mock
        http_hook_mock.run.return_value = Mock(text='success')

        task = RekcurdOperator(task_id='rekcurd_api',
                               dag=self.dag,
                               timeout=100,
                               method='POST',
                               extra_options={'a': 'b'},
                               endpoint='api/test')

        context = None
        result = task.execute(context)

        self.assertEqual(result, 'success')
        http_hook_class_mock.assert_called_with('POST', http_conn_id='rekcurd_dashboard')
        http_hook_mock.run.assert_called_with(
            'api/test', {}, {'Authorization': 'Bearer my_token'}, {'a': 'b', 'timeout': 100}
        )

    @patch("rekcurd_airflow.operators.rekcurd_operator.HttpHook")
    def test_rekcurd_operator_with_extra_timeout_options(self, http_hook_class_mock):
        http_hook_mock = Mock()
        http_hook_class_mock.return_value = http_hook_mock
        http_hook_mock.run.return_value = Mock(text='success')

        task = RekcurdOperator(task_id='rekcurd_api',
                               dag=self.dag,
                               timeout=100,
                               method='POST',
                               extra_options={'a': 'b', 'timeout': 300},
                               endpoint='api/test')

        context = None
        result = task.execute(context)

        self.assertEqual(result, 'success')
        http_hook_class_mock.assert_called_with('POST', http_conn_id='rekcurd_dashboard')
        http_hook_mock.run.assert_called_with(
            'api/test', {}, {'Authorization': 'Bearer my_token'}, {'a': 'b', 'timeout': 300}
        )
