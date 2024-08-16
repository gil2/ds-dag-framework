# Third Party Imports
import time
from logging import Logger

from airflow import AirflowException
from airflow.models import taskinstance
from airflow.sensors.python import PythonSensor
from airflow.utils.db import provide_session
from wix_trino_client.trino_connection import WixTrinoConnection

from python_web_client.web_client import ServerlessWebClient, WebClient

ml_platform_dsg_webclient: WebClient = (
    ServerlessWebClient
    .with_no_auth()
    .from_serverless_application_name('ml-platform-dsg-request')
)


def query_jobs(jobs, endpoint):
    query_response = ml_platform_dsg_webclient.get(url=endpoint, params={'job_ids': jobs})
    if query_response.status_code != 200:
        raise AirflowException("Failed to get job info, error is {}".format(query_response.text))
    return query_response.json()


def wait_on_job(job_id: str, job_type: str, **kwargs):
    JOB_STATE = 'jobState'
    endpoint_mapping = {'bp': 'query_batch_job_states', 'dsg': 'query_job_states'}
    job = query_jobs([job_id], endpoint_mapping[job_type])[0]
    logger = Logger(__file__)
    if job.get(JOB_STATE) == 'END_SUCCESS':
        if job_type == 'dsg':
            kwargs['ti'].xcom_push(key='output_table', value=job.get('datasetTable'))
        return True
    elif job.get(JOB_STATE) == 'END_FAIL':
        raise AirflowException(f"Job {job_id} execution Failed")
    logger.info(f"Job {job_id} is still running")
    return False


def trigger_batch_predict(input_table, output_table, model_name, timeout_minutes=180):
    # install wixml-clients and handle protobuf issue
    import subprocess
    subprocess.run(["python3", "-m", "pip", "install", "protobuf==4.21.1"])
    from wixml_clients import BatchPredictor, BatchPredictorConfig

    timeout_milliseconds = int(timeout_minutes) * 60 * 1000
    predictor = BatchPredictor(BatchPredictorConfig(job_wait_timeout=timeout_milliseconds))

    job_id = predictor.transform_table(model_id=model_name,
                                       input_table_name=input_table,
                                       output_table_name=output_table,
                                       asynchronous=True
                                       )
    return job_id


@provide_session
def clear_tasks(tis, session=None, activate_dag_runs=False, dag=None) -> None:
    """
    Wrapper for `clear_task_instances` to be used in callback function
    (that accepts only `context`)
    """

    taskinstance.clear_task_instances(tis=tis,
                                      session=session,
                                      activate_dag_runs=activate_dag_runs,
                                      dag=dag)


def clear_tasks_callback(context) -> None:
    """
    Clears tasks based on list passed as `task_ids_to_clear` parameter

    To be used as `on_retry_callback`
    """
    print(context)
    all_tasks = context["dag_run"].get_task_instances()
    task_ids_to_clear = context["params"].get("task_ids_to_clear", [])

    tasks_to_clear = [ti for ti in all_tasks if ti.task_id in task_ids_to_clear]

    clear_tasks(tasks_to_clear)


def define_tables(table_list_table, **kwargs):
    pc = WixTrinoConnection(username='wix')
    tables = [elem[0] for elem in
              pc.execute_sql(f"select table_name from {table_list_table} where enriched_table_name is null")]
    return ",".join(tables)


def wait_on_datasets(ti, jobs_table, table_list_table, **kwargs):
    tc = WixTrinoConnection(username='wix')
    job_ids = tc.execute_sql(f"select job_id from {jobs_table}")
    job_ids = [job[0] for job in job_ids]
    is_failed = False
    job_states = query_jobs(job_ids, 'query_job_states')
    kernels_and_enriched = []
    for job in job_states:
        state = job.get('jobState', None)
        if state == "END_SUCCESS":
            kernels_and_enriched.append((job.get('kernel', None),
                                         job.get('datasetTable',
                                                 {})))
        elif state in {"TRIGGERED", "RUNNING"}:
            print(f"The following jobId is still running: {job.get('id')}")
            return False
        else:
            is_failed = True
            continue
    tables = write_output_tables(kernels_and_enriched, is_failed, table_list_table, tc)
    print(tables)
    ti.xcom_push(key='tables', value=tables)
    return True


def write_output_tables(rows, is_failed, table_list_table, tc):
    new = ''
    if rows:
        # write succeeded tables to output column
        selects = [f"select '{dep[0]}' table_name, '{dep[1]}' enriched_table_name" for dep in rows]
        selects = "\nunion\n".join(selects)
        query = f"""
        with new as({selects})

        select old.table_name,coalesce(old.enriched_table_name,new.enriched_table_name) as enriched_table_name
        from {table_list_table} old
        left join new on new.table_name=old.table_name
        """
        new = tc.execute_sql(query)
        res = retry_write(tc, data=new, table=table_list_table)
        print(res)
        print(new)
    if is_failed:
        raise AirflowException(f"Failed Dataset Generation Process")
    tables = ",".join([elem[1] for elem in new])
    return tables


def retry_write(pc, data, table):
    try:
        res = pc.write_data(table_name=table, data=data, col_types=['varchar', 'varchar'],
                            col_names=['table_name', 'enriched_table_name'], if_exists='replace')
    except Exception as e:
        print(e)
        time.sleep(120)
        res = pc.write_data(table_name=table, data=data, col_types=['varchar', 'varchar'],
                            col_names=['table_name', 'enriched_table_name'], if_exists='replace')

    return res


def create_generation_sensor(dag_obj, task_id, task_ids_to_clear, op_kwargs):
    return PythonSensor(
        task_id=task_id,
        python_callable=wait_on_datasets,
        op_kwargs=op_kwargs,
        params=dict(
            task_ids_to_clear=task_ids_to_clear),
        on_retry_callback=clear_tasks_callback,
        retries=0,
        poke_interval=60 * 5,
        retry_delay=10,
        timeout=60 * 60 * 8,
        mode='reschedule',
        do_xcom_push=True,
        dag=dag_obj)


def retrieve_and_drop_temp_tables(table_lists, **kwargs):
    tc = WixTrinoConnection(username='wix')
    all_tables = set()
    for table_list in table_lists:
        query = f"""select table_name from {table_list}
        union select enriched_table_name from {table_list}"""
        tables = tc.execute_sql(query)
        [all_tables.add(elem[0]) for elem in tables]
    drop_queries = [f"""drop table {table};""" for table in all_tables]
    batch_query = f'\n'.join(drop_queries)
    tc.execute_batch_sql(batch_sql=batch_query)
