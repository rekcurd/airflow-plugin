# -*- coding: utf-8 -*-

from airflow.utils.decorators import apply_defaults
from rekcurd_airflow.operators.rekcurd_operator import RekcurdOperator
from airflow.exceptions import AirflowException
import json


class ModelEvaluateOperator(RekcurdOperator):
    """
    evaluate Rekcurd model

    :param app_id: The targetted Rekcurd application ID.
    :type app_id: integer
    :param overwrite: True if overwrite result even if the evaluated result already exists.
    :type overwrite: bool
    :param model_id: ID of the model to be evaluated.
    :type model_id: integer
    :param model_provide_task_id: ID of the task providing model_id by returing value.
        If `model_id` is NOT None, this param is ignored.
    :type model_provide_task_id: string
    :param evaluation_id: ID of the evaluation data.
    :type evaluation_id: integer
    :param evaluation_provide_task_id: ID of the task providing evaluation_id by returing value.
        If `model_id` is NOT None, this param is ignored.
    :type evaluation_provide_task_id: string
    """
    @apply_defaults
    def __init__(self,
                 app_id,
                 timeout=300,
                 overwrite=False,
                 model_id=None,
                 model_provide_task_id=None,
                 evaluation_id=None,
                 evaluation_provide_task_id=None,
                 *args, **kwargs):
        super().__init__(
            endpoint='/api/applications/{}/evaluate'.format(app_id),
            method='POST',
            timeout=timeout,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            *args,
            **kwargs)

        if model_id is None and model_provide_task_id is None:
            raise ValueError('Value must be assigned to either `model_id` or `model_provide_task_id`.')
        if evaluation_id is None and evaluation_provide_task_id is None:
            raise ValueError('Value must be assigned to either `evaluation_id` or `evaluation_provide_task_id`.')
        if model_id is not None and model_provide_task_id is not None:
            msg = '`model_provide_task_id` is ignored because `model_id` is not None'
            self.log.warning(msg)
        if evaluation_id is not None and evaluation_provide_task_id is not None:
            msg = '`evaluation_provide_task_id` is ignored because `evaluation_id` is not None'
            self.log.warning(msg)

        self.__model_id = model_id
        self.__evaluation_id = evaluation_id
        self.__model_task_id = model_provide_task_id
        self.__evaluation_task_id = evaluation_provide_task_id
        self.__overwrite = overwrite

    def execute(self, context):
        if self.__model_id is None:
            self.__model_id = context['ti'].xcom_pull(task_ids=self.__model_task_id)
        if self.__evaluation_id is None:
            self.__evaluation_id = context['ti'].xcom_pull(task_ids=self.__evaluation_task_id)

        self.data = "model_id={}&evaluation_id={}&overwrite={}".format(
            self.__model_id, self.__evaluation_id, str(self.__overwrite).lower())

        result = json.loads(super().execute(context))
        if result.get('status'):
            self.log.info('succcessfully evaluated.')
        else:
            raise AirflowException('failed to evaluate.')
        return result
