"""
master dag = scheduled procedure for generating DAGs using a list of datasets
source: https://airflow.apache.org/docs/apache-airflow/stable/howto/dynamic-dag-generation.html
"""
from datetime import datetime

from airflow import DAG
from airflow.decorators import task, dag
from airflow.operators.empty import EmptyOperator
from airflow.utils.edgemodifier import Label
from airflow.utils.trigger_rule import TriggerRule

from connector import SRC_MAP
from dataset import Datasets

for dataset in Datasets:
    dag_id = f"dag_{dataset.name}"

    @dag(dag_id=dag_id, start_date=datetime(2022,8,1))
    def dynamic_generated_dag():
        @task(task_id=f"{dag_id}_entry")
        def print_start():
            print(f"starting run for {dag_id}")

        start_task = print_start()

        join = EmptyOperator(
            task_id='join',
            trigger_rule=TriggerRule.ALL_SUCCESS,
        )

        data_providers = dataset.get_providers()

        for native_id in data_providers:
            @task(task_id=f"{dag_id}_connector")
            def connector():
                return SRC_MAP[dataset.connector](native_id)

            connector_task = connector()

            @task(task_id=f"{dag_id}_executor")
            def executor():
                return dataset.executor(native_id, dataset.name)

            executor_task = executor()

            start_task >>connector_task >> executor_task >> join

    globals()[dag_id] = dynamic_generated_dag()
    
