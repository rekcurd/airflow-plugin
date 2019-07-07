import unittest
from datetime import datetime
from unittest.mock import Mock, patch
from airflow import DAG
from airflow.models import Variable
from rekcurd_airflow.operators import EvaluationUploadOperator
from tempfile import NamedTemporaryFile


class TestEvaluationUploadOperator(unittest.TestCase):
    @staticmethod
    def setUpClass():
        Variable.set('rekcurd_access_token', 'my_token')

    @staticmethod
    def tearDownClass():
        Variable.set('rekcurd_access_token', None)

    def setUp(self):
        self.dag = DAG(dag_id='test_evaluation_upload_operator',
                       start_date=datetime.now())

    @patch("rekcurd_airflow.operators.evaluation_upload_operator.Request")
    @patch("rekcurd_airflow.operators.evaluation_upload_operator.HttpHook")
    def test_upload(self, http_hook_class_mock, request_class_mock):
        prepare_request_mock = Mock()
        session_mock = Mock()
        session_mock.prepare_request.return_value = prepare_request_mock

        http_hook_mock = Mock(base_url='http://rekcurd-dashboard.com')
        http_hook_mock.get_conn.return_value = session_mock
        http_hook_mock.run_and_check.return_value = Mock(
            text='{"status": true, "evaluation_id": 10}')
        http_hook_class_mock.return_value = http_hook_mock

        request_mock = Mock()
        request_class_mock.return_value = request_mock

        with NamedTemporaryFile() as tf:
            tf.write(b'hello foo.')
            tf.seek(0)

            task = EvaluationUploadOperator(task_id='rekcurd_api',
                                            dag=self.dag,
                                            project_id=1, app_id='sample_app',
                                            evaluation_file_path=tf.name,
                                            description='sample data')
            task.execute(None)
            expected_headers = {'Authorization': 'Bearer my_token'}

            http_hook_class_mock.assert_called_with('POST', http_conn_id='rekcurd_dashboard')
            http_hook_mock.get_conn.assert_called_with(expected_headers)
            session_mock.prepare_request.assert_called_with(request_mock)
            request_class_mock.assert_called_with(
                'POST',
                'http://rekcurd-dashboard.com/api/projects/1/applications/sample_app/evaluations',
                files={'filepath': b'hello foo.'},
                data={'description': 'sample data'},
                headers=expected_headers)
            http_hook_mock.run_and_check.assert_called_with(
                session_mock,
                prepare_request_mock,
                {'timeout': 300})
