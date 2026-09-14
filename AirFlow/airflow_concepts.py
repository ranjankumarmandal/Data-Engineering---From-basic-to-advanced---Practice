from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import dag, task
from airflow.models import Variable, Param
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.standard.operators.python import PythonVirtualenvOperator
from airflow.providers.standard.sensors.time_delta import TimeDeltaSensor
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def extract_data(**context):
    execution_date = context["logical_date"]

    data = {
        "customers": 1000,
        "orders": 5000,
        "execution_date": str(execution_date),
    }

    print(f"Extracted data: {data}")

    return data


def transform_data(**context):
    ti = context["ti"]

    data = ti.xcom_pull(task_ids="extract_data")

    if data is None:
        raise ValueError("No data received from extract_data")

    transformed_data = {
        "customers": data["customers"],
        "orders": data["orders"],
        "total_records": data["customers"] + data["orders"],
    }

    print(f"Transformed data: {transformed_data}")

    return transformed_data


def load_data(**context):
    ti = context["ti"]

    data = ti.xcom_pull(task_ids="transform_data")

    print(f"Loading data: {data}")


def check_data_quality(**context):
    ti = context["ti"]

    data = ti.xcom_pull(task_ids="transform_data")

    if data["total_records"] > 0:
        return "data_quality_pass"
    else:
        return "data_quality_fail"


def data_quality_pass():
    print("Data quality check passed")


def data_quality_fail():
    print("Data quality check failed")


def send_notification(**context):
    print("Pipeline completed successfully")


def create_report(**context):
    ti = context["ti"]

    data = ti.xcom_pull(task_ids="transform_data")

    print("Creating data engineering report")
    print(f"Report data: {data}")


def process_partition(partition):
    print(f"Processing partition: {partition}")


with DAG(
    dag_id="airflow_concepts",
    default_args=default_args,
    description="A single DAG demonstrating major Airflow concepts",
    start_date=datetime(2025, 1, 1),
    schedule="0 6 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["airflow", "data-engineering", "learning"],
    params={
        "environment": Param(
            "dev",
            type="string",
            enum=["dev", "test", "prod"],
        ),
        "process_date": Param(
            "2025-01-01",
            type="string",
        ),
    },
) as dag:

    start = EmptyOperator(
        task_id="start",
    )

    extract_data = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data,
    )

    transform_data = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
    )

    load_data = PythonOperator(
        task_id="load_data",
        python_callable=load_data,
    )

    run_bash_command = BashOperator(
        task_id="run_bash_command",
        bash_command="echo 'Running Bash command from Airflow'",
    )

    print_python_value = PythonOperator(
        task_id="print_python_value",
        python_callable=lambda: print("PythonOperator executed"),
    )

    with TaskGroup(
        group_id="data_quality",
        tooltip="Data quality checks",
    ) as data_quality:

        check_data = BranchPythonOperator(
            task_id="check_data",
            python_callable=check_data_quality,
        )

        data_quality_pass = PythonOperator(
            task_id="data_quality_pass",
            python_callable=data_quality_pass,
        )

        data_quality_fail = PythonOperator(
            task_id="data_quality_fail",
            python_callable=data_quality_fail,
        )

        check_data >> [data_quality_pass, data_quality_fail]

    wait_before_processing = TimeDeltaSensor(
        task_id="wait_before_processing",
        delta=timedelta(seconds=10),
    )

    with TaskGroup(
        group_id="parallel_processing",
        tooltip="Parallel processing tasks",
    ) as parallel_processing:

        process_customers = PythonOperator(
            task_id="process_customers",
            python_callable=process_partition,
            op_kwargs={"partition": "customers"},
        )

        process_orders = PythonOperator(
            task_id="process_orders",
            python_callable=process_partition,
            op_kwargs={"partition": "orders"},
        )

        process_products = PythonOperator(
            task_id="process_products",
            python_callable=process_partition,
            op_kwargs={"partition": "products"},
        )

        [process_customers, process_orders, process_products]

    create_report = PythonOperator(
        task_id="create_report",
        python_callable=create_report,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    send_notification = PythonOperator(
        task_id="send_notification",
        python_callable=send_notification,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    start >> extract_data

    extract_data >> transform_data

    transform_data >> load_data

    load_data >> run_bash_command

    run_bash_command >> print_python_value

    print_python_value >> data_quality

    data_quality >> wait_before_processing

    wait_before_processing >> parallel_processing

    parallel_processing >> create_report

    create_report >> send_notification

    send_notification >> end
