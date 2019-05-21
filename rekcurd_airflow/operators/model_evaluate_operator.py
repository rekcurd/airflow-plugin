# -*- coding: utf-8 -*-
import json
from typing import Optional

from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException

from rekcurd_airflow.operators.rekcurd_operator import RekcurdOperator


class ModelEvaluateOperator(RekcurdOperator):
    """
    evaluate Rekcurd model

    :param project_id: The targetted Rekcurd project ID.
    :param app_id: The targetted Rekcurd application ID.
    :param overwrite: True if overwrite result even if the evaluated result already exists.
    :param model_id: ID of the model to be evaluated.
    :param model_provide_task_id: ID of the task providing model_id by returing value.
        If `model_id` is NOT None, this param is ignored.
    :param evaluation_id: ID of the evaluation data.
    :param evaluation_provide_task_id: ID of the task providing evaluation_id by returing value.
        If `model_id` is NOT None, this param is ignored.
    """
    @apply_defaults
    def __init__(self,
                 project_id: int,
                 app_id: str,
                 timeout: int = 300,
                 overwrite: bool = False,
                 model_id: Optional[int] = None,
                 model_provide_task_id: Optional[str] = None,
                 evaluation_id: Optional[int] = None,
                 evaluation_provide_task_id: Optional[str] = None,
                 *args, **kwargs):
        super().__init__(
            endpoint=self._base_app_endpoint(project_id, app_id) + 'evaluate',
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
