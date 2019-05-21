# -*- coding: utf-8 -*-
from typing import Dict, Optional, Any, Union

from airflow.utils.decorators import apply_defaults
from airflow.hooks.http_hook import HttpHook
from airflow.models import BaseOperator
from airflow.models import Variable


class RekcurdOperator(BaseOperator):
    """
    Add authorization information to HTTP header of SimpleHttpOperator

    :param endpoint: The relative part of the full url.
    :param timeout: Timeout of HTTP request (unit is second.)
    :param extra_options: Extra options for the 'requests' library, see the
        'requests' documentation (options to modify timeout, ssl, etc.)
    """
    @apply_defaults
    def __init__(self,
                 endpoint: str,
                 timeout: int,
                 method: str,
                 headers: Dict[str, str] = {},
                 data: Union[str, Dict[str, Any]] = {},
                 extra_options: Optional[Dict[str, Any]] = None,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)

        if extra_options is not None and 'timeout' not in extra_options:
            extra_options['timeout'] = timeout
        else:
            extra_options = {'timeout': timeout}

        self.http_conn_id = 'rekcurd_dashboard'
        self.endpoint = endpoint
        self.extra_options = extra_options
        self.method = method
        self.headers = headers
        self.data = data

        # FIXME: JWT is used as access token for now
        self.headers['Authorization'] = 'Bearer {}'.format(
            Variable.get('rekcurd_access_token')
        )

    def _base_app_endpoint(self, project_id: int, app_id: str):
        return '/api/projects/{}/applications/{}/'.format(project_id, app_id)

    def execute(self, context) -> Any:
        http = HttpHook(self.method, http_conn_id=self.http_conn_id)

        response = http.run(self.endpoint,
                            self.data,
                            self.headers,
                            self.extra_options)
        return response.text
