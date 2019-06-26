# -*- coding: utf-8 -*-
import json
from requests import Request
from urllib.parse import urljoin

from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
from airflow.hooks.http_hook import HttpHook

from rekcurd_airflow.operators.rekcurd_operator import RekcurdOperator


class EvaluationUploadOperator(RekcurdOperator):
    """
    Upload evaluation data

    :param project_id: The targetted Rekcurd project ID.
    :param app_id: The targetted Rekcurd application ID.
    :param evaluation_file_path: File path to evaluation data to be uploaded.
    """
    @apply_defaults
    def __init__(self,
                 project_id: int,
                 app_id: str,
                 evaluation_file_path: str,
                 timeout: int = 300,
                 *args, **kwargs):
        super().__init__(
            endpoint=self._base_app_endpoint(project_id, app_id) + 'evaluations',
            timeout=timeout,
            method='POST',
            *args,
            **kwargs)

        self.__evaluation_path = evaluation_file_path

    def execute(self, context) -> int:
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
