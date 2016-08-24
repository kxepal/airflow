# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import datetime
import inspect
import importlib
import yaml
from airflow.models import DAG, BaseOperator
from airflow.operators.sensors import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class CallbackMixIn(object):

    _callback = None

    def callback(self, context):
        if inspect.isclass(self._callback):
            obj = self._callback()
        else:
            obj = self._callback
        obj(context)


class GenericOperator(BaseOperator, CallbackMixIn):

    @apply_defaults
    def __init__(self, _callback, *args, **kwargs):
        super(GenericOperator, self).__init__(*args, **kwargs)
        self._callback = _callback

    def execute(self, context):
        self.callback(context)


class GenericSensor(BaseSensorOperator, CallbackMixIn):

    @apply_defaults
    def __init__(self, _callback, *args, **kwargs):
        super(GenericSensor, self).__init__(*args, **kwargs)
        self._callback = _callback

    def poke(self, context):
        self.callback(context)


def load_dags(path):
    return process_scheme(read_scheme(path))


def read_scheme(path):
    with open(path) as fobj:
        return yaml.safe_load(fobj)


def process_scheme(scheme):
    return [build_dag(dag_id, dag_scheme)
            for dag_id, dag_scheme in scheme['dags'].items()]


def build_dag(dag_id, scheme):
    dag = DAG(dag_id=dag_id, **transform_args(scheme.get('args', {})))
    sensors = {
        sensor_id: build_sensor(dag, sensor_id, sensor_scheme)
        for sensor_id, sensor_scheme in scheme.get('sensors', {}).items()
    }
    operators = {
        operator_id: build_operator(dag, operator_id, operator_scheme)
        for operator_id, operator_scheme in scheme.get('operators', {}).items()
    }
    build_flow(dict(operators, **sensors), scheme.get('flow', {}))
    return dag


def build_sensor(dag, sensor_id, sensor_scheme):
    return build_operator(dag, sensor_id, sensor_scheme, GenericSensor)


def build_operator(dag, operator_id, scheme, operator_class=GenericOperator):
    args = transform_args(scheme.get('args', {}))

    obj_name = scheme.get('class', None)
    if obj_name is not None:
        operator_class = import_object(obj_name)
        return operator_class(task_id=operator_id, dag=dag, **args)

    callback_name = scheme.get('callback', None)
    if callback_name is not None:
        callback = import_object(callback_name)
        return operator_class(callback, task_id=operator_id, dag=dag, **args)

    raise RuntimeError('nothing to do with %s: %s' % (operator_id, scheme))


def build_flow(tasks, scheme):
    for idx, upstream in scheme.items():
        tasks[idx].set_upstream(tasks[task_id] for task_id in upstream)


def transform_args(args):
    for key, func in [('default_args', transform_args),
                      ('start_date', transform_date),
                      ('end_date', transform_date),
                      ('schedule_interval', transform_schedule_interval),
                      ]:
        transform(args, key, func)
    return args


def transform_date(value):
    if isinstance(value, datetime.date):
        time = datetime.datetime.min.time()
        value = datetime.datetime.combine(value, time)
    return value


def transform_schedule_interval(value):
    if isinstance(value, int):
        value = datetime.timedelta(seconds=value)
    return value


def transform(args, key, func):
    if key in args:
        args[key] = func(args[key])


def import_object(value):
    module, object_name = value.split(':', 1)
    mod = importlib.import_module(module)
    return getattr(mod, object_name)
