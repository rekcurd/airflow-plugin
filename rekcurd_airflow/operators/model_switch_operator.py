# -*- coding: utf-8 -*-

from airflow.utils.decorators import apply_defaults
from rekcurd_airflow.operators.rekcurd_operator import RekcurdOperator
from airflow.exceptions import AirflowException
import json


class ModelSwitchOperator(RekcurdOperator):
    """
    Switch Rekcurd model

    :param app_id: The targetted Rekcurd application ID.
    :type app_id: integer
    :param service_id: The targetted Rekcurd service ID.
    :type service_id: integer
    :param model_id: ID of the model to be switched to.
        The targetted service will use this model.
    :type model_id: integer
    :param model_provide_task_id: ID of the task providing model_id by returing value.
        If `model_id` is NOT None, this param is ignored.
    :type model_provide_task_id: string
    """
    @apply_defaults
    def __init__(self,
                 app_id,
                 service_id,
                 timeout=300,
                 model_id=None,
                 model_provide_task_id=None,
                 *args, **kwargs):
        super().__init__(
            endpoint='/api/applications/{}/services/{}'.format(app_id, service_id),
            method='PUT',
            timeout=timeout,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            *args,
            **kwargs)

        if model_id is None and model_provide_task_id is None:
            raise ValueError('Value must be assigned to either `model_id` or `model_provide_task_id`.')
        if model_id is not None and model_provide_task_id is not None:
            msg = '`model_provide_task_id` is ignored because `model_id` is not None'
            self.log.warning(msg)

        self.__model_id = model_id
        self.__xcom_task_id = model_provide_task_id

    def execute(self, context):
        if self.__model_id is None:
            self.__model_id = context['ti'].xcom_pull(task_ids=self.__xcom_task_id)
        self.data = "model_id={}".format(self.__model_id)

        result = json.loads(super().execute(context))
        if result.get('status'):
            self.log.info(result['message'])
        else:
            raise AirflowException(result['message'])
