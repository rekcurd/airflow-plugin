# -*- coding: utf-8 -*-

from airflow.utils.decorators import apply_defaults
from rekcurd_airflow.operators.rekcurd_operator import RekcurdOperator
from airflow.exceptions import AirflowException
from airflow.hooks.http_hook import HttpHook
import json
from requests import Request
from urllib.parse import urljoin


class EvaluationUploadOperator(RekcurdOperator):
    """
    Upload evaluation data

    :param app_id: The targetted Rekcurd application ID.
    :type app_id: integer
    :param evaluation_file_path: file path to evaluation data to be uploaded
    :type evaluation_file_path: string
    """
    @apply_defaults
    def __init__(self,
                 app_id,
                 timeout=300,
                 evaluation_file_path=None,
                 *args, **kwargs):
        super().__init__(
            endpoint='/api/applications/{}/evaluations'.format(app_id),
            timeout=timeout,
            method='POST',
            *args,
            **kwargs)

        self.__evaluation_path = evaluation_file_path

    def execute(self, context):
        http = HttpHook(self.method, http_conn_id=self.http_conn_id)
        session = http.get_conn(self.headers)
        with open(self.__evaluation_path, 'rb') as evaluation_file:
            evaluation_data = evaluation_file.read()

        req = Request(self.method,
                      urljoin(http.base_url, self.endpoint),
                      files={'file': evaluation_data},
                      headers=self.headers)

        response = http.run_and_check(session,
                                      session.prepare_request(req),
                                      self.extra_options)
        result = json.loads(response.text)

        if result['status']:
            self.log.info(f'Success. evaluation_id: {result["evaluation_id"]}')
        else:
            raise AirflowException(result['message'])

        return result["evaluation_id"]
