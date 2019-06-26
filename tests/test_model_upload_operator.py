import unittest
import json
from datetime import datetime
from unittest.mock import Mock, patch, call
from airflow import DAG
from airflow.models import Variable
from rekcurd_airflow.operators import ModelUploadOperator
from tempfile import NamedTemporaryFile


class TestModelUploadOperator(unittest.TestCase):
    @staticmethod
    def setUpClass():
        Variable.set('rekcurd_access_token', 'my_token')

    @staticmethod
    def tearDownClass():
        Variable.set('rekcurd_access_token', None)

    def setUp(self):
        self.dag = DAG(dag_id='test_model_upload_operator', start_date=datetime.now())

    def test_get_model_data(self):
        with NamedTemporaryFile() as tf:
            tf.write(b'dummy model content')
            tf.seek(0)
            task = ModelUploadOperator(task_id='rekcurd_api',
                                       dag=self.dag,
                                       project_id=1, app_id='sample_app',
                                       model_file_path=tf.name,
                                       model_description='dummy model')
            model, desc = task.get_model_data(None)

        self.assertEqual(model, b'dummy model content')
        self.assertEqual(desc, 'dummy model')

    def test_get_model_data_from_xcom(self):
        task = ModelUploadOperator(task_id='rekcurd_api',
                                   dag=self.dag,
                                   project_id=1, app_id='sample_app',
                                   model_provide_task_id='task_1',
                                   model_description='dummy model')
        ti_mock = Mock()
        ti_mock.xcom_pull.return_value = 'model binary'
        context = {'ti': ti_mock}

        model, desc = task.get_model_data(context)
        self.assertEqual(model, 'model binary')
        self.assertEqual(desc, 'model binary')

        ti_mock.xcom_pull.assert_has_calls([
            call(key='rekcurd_model_key', task_ids='task_1'),
            call(key='rekcurd_model_desc_key', task_ids='task_1')
        ])

    @patch("rekcurd_airflow.operators.model_upload_operator.Request")
    @patch("rekcurd_airflow.operators.model_upload_operator.HttpHook")
    def test_upload(self, http_hook_class_mock, request_class_mock):
        prepare_request_mock = Mock()

        session_mock = Mock()
        session_mock.prepare_request.return_value = prepare_request_mock

        http_hook_mock = Mock(base_url='http://rekcurd-dashboard.com')
        http_hook_mock.get_conn.return_value = session_mock
        http_hook_mock.run_and_check.return_value = Mock(
            text='{"status": true, "message": "success"}')

        http_hook_class_mock.return_value = http_hook_mock

        request_mock = Mock()
        request_class_mock.return_value = request_mock
        task = ModelUploadOperator(task_id='rekcurd_api',
                                   dag=self.dag,
                                   project_id=1, app_id='sample_app',
                                   model_file_path='test.zip')
        model = 'dummy model'
        desc = 'dummy desc'
        task.upload(model, desc)

        expected_headers = {'Authorization': 'Bearer my_token'}

        http_hook_class_mock.assert_called_with('POST', http_conn_id='rekcurd_dashboard')
        http_hook_mock.get_conn.assert_called_with(expected_headers)
        session_mock.prepare_request.assert_called_with(request_mock)
        request_class_mock.assert_called_with(
            'POST',
            'http://rekcurd-dashboard.com/api/projects/1/applications/sample_app/models',
            data={'description': desc},
            files={'file': model},
            headers=expected_headers)
        http_hook_mock.run_and_check.assert_called_with(
            session_mock,
            prepare_request_mock,
            {'timeout': 300})

    @patch("rekcurd_airflow.operators.model_upload_operator.HttpHook")
    def test_get_model_id(self, http_hook_class_mock):
        http_hook_mock = Mock(base_url='http://rekcurd-dashboard.com')
        run_result = [
            {"model_id": 5, "register_date": 2.0, "description": "dummy1"},
            {"model_id": 6, "register_date": 5.0, "description": "dummy2"},
            {"model_id": 3, "register_date": 1.0, "description": "dummy1"},
            {"model_id": 4, "register_date": 1.5, "description": "dummy2"},
        ]
        http_hook_mock.run.return_value = Mock(text=json.dumps(run_result))
        http_hook_class_mock.return_value = http_hook_mock

        task = ModelUploadOperator(task_id='rekcurd_api',
                                   dag=self.dag,
                                   project_id=1, app_id='sample_app',
                                   model_file_path='test.zip')
        res = task.get_model_id('dummy2')
        self.assertEqual(res, 6)

        expected_headers = {'Authorization': 'Bearer my_token'}

        http_hook_class_mock.assert_called_with('GET', http_conn_id='rekcurd_dashboard')
        http_hook_mock.run.assert_called_with(
            '/api/projects/1/applications/sample_app/models',
            headers=expected_headers,
            extra_options={'timeout': 300})
