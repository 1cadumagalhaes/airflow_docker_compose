#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Example DAG demonstrating the usage of dynamic task mapping."""

from __future__ import annotations

from datetime import datetime

from airflow.sdk import dag, task, task_group


@dag(
    default_args={'owner': 'airflow'},
    dag_id='dynamic_task_mapping',
    dag_display_name='09 - Dynamic Task Mapping',
    schedule=None,
    start_date=datetime(2022, 3, 4),
    tags=['example'],
    owner_links={'airflow': 'https://airflow.apache.org'},
)
def dynamic_task_mapping():

    @task
    def add_one(x: int):
        return x + 1

    @task
    def sum_it(values):
        total = sum(values)
        print(f'Total was {total}')

    added_values = add_one.expand(x=[1, 2, 3])
    sum_it(added_values)


@dag(
    dag_id='dynamic_task_mapping_second_order',
    dag_display_name='09 - Dynamic Task Mapping (second order)',
    schedule=None,
    catchup=False,
    start_date=datetime(2022, 3, 4),
    tags=['example'],
)
def dynamic_task_mapping_second_order():

    @task
    def get_nums():
        return [1, 2, 3]

    @task
    def times_2(num):
        return num * 2

    @task
    def add_10(num):
        return num + 10

    _get_nums = get_nums()
    _times_2 = times_2.expand(num=_get_nums)
    add_10.expand(num=_times_2)


@dag(
    dag_id='dynamic_task_group_mapping',
    dag_display_name='09 - Dynamic Task Mapping (with Task Groups)',
    schedule=None,
    catchup=False,
    start_date=datetime(2022, 3, 4),
    tags=['example'],
)
def dynamic_task_group_mapping():

    @task_group
    def op(num):
        @task
        def add_1(num):
            return num + 1

        @task
        def mul_2(num):
            return num * 2

        return mul_2(add_1(num))

    op.expand(num=[1, 2, 3])


dynamic_task_mapping()
dynamic_task_mapping_second_order()
dynamic_task_group_mapping()
