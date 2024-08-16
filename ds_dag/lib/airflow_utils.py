import os
import urllib.parse

def urlify(string):
    return urllib.parse.quote(string)

def get_dag_and_task_links(dag_id, task_id, execution_date=None):
    execution_date = execution_date or os.getenv('AIRFLOW_CTX_EXECUTION_DATE')
    execution_date = urlify(execution_date)
    dag_link = f"https://bo.wix.com/airflow-prod/graph?dag_id={dag_id}&root=&execution_date={execution_date}"
    task_link = f"https://bo.wix.com/airflow-prod/log?dag_id={dag_id}&task_id={task_id}&execution_date={execution_date}"
    print(f"dag_link = {dag_link}")
    return dag_link, task_link
