# -*- coding: utf-8 -*-

from airflow.utils.decorators import apply_defaults
from rekcurd_airflow.operators.rekcurd_operator import RekcurdOperator
from airflow.exceptions import AirflowException
from airflow.hooks.http_hook import HttpHook
import json
from requests import Request
from urllib.parse import urljoin


class ModelUploadOperator(RekcurdOperator):
    MODEL_KEY = 'rekcurd_model_key'
    MODEL_DESCRIPTION_KEY = 'rekcurd_model_desc_key'

    """
    Upload Rekcurd model

    :param app_id: The targetted Rekcurd application ID.
    :type app_id: integer
    :param model_file_path: file path to the model to be uploaded
    :type model_file_path: string
    :param model_description: description of the model
    :type model_description: string
    :param model_provide_task_id: ID of the task providing model information by xcom_push
        If `model_file_path` is NOT None, this param is ignored.
    """
    @apply_defaults
    def __init__(self,
                 app_id,
                 timeout=300,
                 model_file_path=None,
                 model_description=None,
                 model_provide_task_id=None,
                 *args, **kwargs):
        super().__init__(
            endpoint='/api/applications/{}/models'.format(app_id),
            timeout=timeout,
            method=None,
            *args,
            **kwargs)

        if model_file_path is None and model_provide_task_id is None:
            raise ValueError('Value must be assigned to either `model_file_path` or `model_provide_task_id`.')
        if model_file_path is not None and model_provide_task_id is not None:
            self.log.warning('`model_provide_task_id` is ignored because `model_file_path` is not None')

        self.__model_path = model_file_path
        self.__desc = model_description
        self.__xcom_task_id = model_provide_task_id

    def execute(self, context):
        model, desc = self.get_model_data(context)
        result = json.loads(self.upload(model, desc))

        if result.get('status'):
            self.log.info(result['message'])
        else:
            raise AirflowException(result['message'])

        model_id = self.get_model_id(desc)
        self.log.info('ID of the uploaded model: {}'.format(model_id))
        return model_id

    def get_model_data(self, context):
        if self.__model_path is None:
            model = context['ti'].xcom_pull(
                    key=self.MODEL_KEY,
                    task_ids=self.__xcom_task_id)
            desc = context['ti'].xcom_pull(
                    key=self.MODEL_DESCRIPTION_KEY,
                    task_ids=self.__xcom_task_id)
            if desc is None:
                desc = self.__desc
        else:
            with open(self.__model_path, 'rb') as modelfile:
                model = modelfile.read()
            desc = self.__desc
        return model, desc

    def upload(self, model, desc):
        http = HttpHook('POST', http_conn_id=self.http_conn_id)
        session = http.get_conn(self.headers)
        req = Request('POST',
                      urljoin(http.base_url, self.endpoint),
                      data={'description': desc},
                      files={'file': model},
                      headers=self.headers)
        prepped_request = session.prepare_request(req)

        response = http.run_and_check(session, prepped_request, self.extra_options)
        return response.text

    def get_model_id(self, desc):
        http = HttpHook('GET', http_conn_id=self.http_conn_id)
        response = http.run(self.endpoint, headers=self.headers, extra_options=self.extra_options)
        models = json.loads(response.text)

        model = None
        for m in models:
            if m['description'] == desc:
                if model is None or model['register_date'] < m['register_date']:
                    model = m

        if model is None:
            raise AirflowException('The uploaded model was not found in Rekcurd')

        return model['model_id']
