# -*- coding: utf-8 -*-

from airflow.utils.decorators import apply_defaults
from airflow.hooks.http_hook import HttpHook
from airflow.models import BaseOperator
from airflow.models import Variable


class RekcurdOperator(BaseOperator):
    """
    Add authorization information to HTTP header of SimpleHttpOperator

    :param endpoint: The relative part of the full url. (templated)
    :type endpoint: string
    :param timeout: Timeout of HTTP request (unit is second)
    :type timeout: integer
    :param extra_options: Extra options for the 'requests' library, see the
        'requests' documentation (options to modify timeout, ssl, etc.)
    :type extra_options: A dictionary of options, where key is string and value
        depends on the option that's being modified.
    """
    @apply_defaults
    def __init__(self,
                 endpoint,
                 timeout,
                 method,
                 headers={},
                 data={},
                 extra_options=None,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)

        if isinstance(extra_options, dict):
            if 'timeout' not in extra_options:
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

    def execute(self, context):
        http = HttpHook(self.method, http_conn_id=self.http_conn_id)

        response = http.run(self.endpoint,
                            self.data,
                            self.headers,
                            self.extra_options)
        return response.text
