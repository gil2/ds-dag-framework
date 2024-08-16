# Third Party Imports
from datetime import datetime, timedelta
from logging import Logger
from textwrap import dedent

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonVirtualenvOperator, BranchPythonOperator, PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.edgemodifier import Label
from airflow.utils.trigger_rule import TriggerRule
from wixflow.operators.platyspark_cluster_config import SharedClusterConfig
from wixflow.operators.platyspark_operators import PlatySparkRunAppOperator, LocalLocation
from wixflow.operators.trino import TrinoOperator
from wixflow.utils.slack_helper_functions import generic_on_failure_callback, generic_on_success_callback

# project imports
from ds_dag.dags.dag_constants import DagConfKeys, TmpTableKeys, FilePaths
from ds_dag.dags.doc_creator import DSDocCreator
from ds_dag.dags.jobs_manager import trigger_batch_predict, wait_on_job
from ds_dag.lib.github_api import trigger_github_workflow, check_github_workflow_status

logger = Logger(__file__)

class DSDag:
    def __init__(
            self,
            dag_id,
            ds_dag_manager,
            run_type=None,  # Should only be passed for Orchestrator, otherwise will remain None, to be taken from dag_run
            doc_file='doc.md'
    ):
        self.dag_id = dag_id
        self.dag = None
        self.ds_dag_manager = ds_dag_manager
        self.run_type = run_type
        self.prev_task = None
        self.tables_to_drop = []
        self.empty_task_index = 1
        self.dropped_rows_table = self.get_from_dag_run(DagConfKeys.DROPPED_ROWS_TABLE)
        self.input_table = self.dag_input_table = self.current_output_table = None
        slack_channel = self.get_from_config_file(DagConfKeys.SLACK_CHANNEL)
        self.default_args = {
            "owner": "ds-data-engineering-team",
            "start_date": datetime(2023, 7, 1),
            "email": self.get_from_config_file(DagConfKeys.EMAILS),
            "email_on_failure": True,
            "email_on_retry": True,
            "execution_timeout": timedelta(hours=3),
            "dagrun_timeout": timedelta(hours=5),
            "retries": 1,
            "retry_delay": timedelta(minutes=10),
            'on_failure_callback': generic_on_failure_callback(slack_channel),
            'on_success_callback': generic_on_success_callback(slack_channel),
        }
        self.history_table = None
        self.md = DSDocCreator(ds_dag=self, doc_file=doc_file)

    @property
    def config_file(self):
        return self.ds_dag_manager.config_file

    @property
    def model_input_table_all_fields(self):
        return self.get_tmp_table_name(key=TmpTableKeys.MODEL_INPUT_TABLE_ALL_FIELDS)

    @property
    def model_input_table(self):
        return self.get_tmp_table_name(key=TmpTableKeys.MODEL_INPUT_TABLE)

    @property
    def model_output_table(self):
        return self.get_tmp_table_name(key=TmpTableKeys.MODEL_OUTPUT_TABLE)

    @property
    def output_table(self):
        return self.get_tmp_table_name(key=TmpTableKeys.OUTPUT_TABLE)

    def get_tmp_table_name(self, key=None):
        table_name = self.config_file.get_tmp_table_name(
            dag_id=self.dag_id,
            key=key,
            run_time_and_type=self.run_time_and_type
        )
        return table_name

    @property
    def run_time_and_type(self):
        return self.get_from_dag_run(DagConfKeys.RUN_TIME_AND_TYPE)

    @property
    def model_name(self):
        return self.get_from_dag_run(DagConfKeys.MODEL)

    @property
    def model_short_name(self):
        return self.get_from_dag_run(DagConfKeys.SHORT_NAME)

    def get_output_table(self, dag_id):
        return self.config_file.get_tmp_table_name(
            dag_id=dag_id,
            key=TmpTableKeys.OUTPUT_TABLE,
            run_time_and_type=self.run_time_and_type
        )

    def get_history_table(self, key=DagConfKeys.HISTORY_TABLE):
        return self.history_table or self.get_from_dag_run(key=key)

    def _child_create_dag(self):
        # params = "{{ dag_run.conf.get('{key}') }}"
        params = self.config_file.get_dag_value(
            dag_id=self.dag_id,
            key=DagConfKeys.PARAMS,
            default_val={}
        )
        # params.delete(DagConfKeys.ESSENCE)
        # params['history_table'] = self.get_default_history_table_name(short_name=self.get_from_config_file(DagConfKeys.SHORT_NAME))
        self.dag = DAG(
            dag_id=self.dag_id,
            start_date=datetime(2023, 12, 1),
            default_args=self.default_args,
            max_active_runs=5,
            schedule=self.get_from_config_file(DagConfKeys.SCHEDULE),
            render_template_as_native_obj=True,
            params=params
        )

    def _child_setup_created_dag(self):
        input_dag = self.get_from_config_file(key=DagConfKeys.INPUT_DAG)
        if input_dag:
            self.dag_input_table = self.get_output_table(dag_id=input_dag)
        else:
            self.dag_input_table = self.get_from_dag_run(key=DagConfKeys.INPUT_TABLE)

        self.current_output_table = self.input_table = self.dag_input_table

    def create_dag(self):
        self._child_create_dag()
        with self.dag as dag:
            dag.tags = self.get_from_config_file(DagConfKeys.TAGS)

            self.prev_task = EmptyOperator(task_id='kickoff')
            self._child_setup_created_dag()

            globals()[self.dag_id] = dag
            return dag

    def add_to_dependencies_and_doc(self, task, description="", add_to_doc=True):
        self.prev_task >> task
        self.prev_task = task

        if add_to_doc:
            task_id = task.task_id if hasattr(task, 'task_id') else task.group_id
            self.md.add_task(task_id=task_id, description=description)

    def set_tables(self, input_table, output_table, output_table_key=None, is_dag_output_table=False):
        self.input_table = input_table or self.current_output_table
        if is_dag_output_table:
            self.current_output_table = output_table or self.output_table
        else:
            self.current_output_table = output_table or self.get_tmp_table_name(key=output_table_key)
            self.tables_to_drop.append(self.current_output_table)

    def new_task_create_table(
        self,
        task_id='create_input_table',
        input_table=None,
        output_table=None,
        output_table_key=TmpTableKeys.MODEL_INPUT_TABLE,
        is_dag_output_table=False,
        sql=None,
        sql_index=None,
        lib_path=FilePaths.lib_path,
        path_from_github_root=FilePaths.lib_path_from_github_root,
        entrypoint=DagConfKeys.CREATE_TABLE_ENTRYPOINT,
        additional_args=None,
        op_kwargs=None,  # For when calling by sql index,
        use_spark=True,
        description="",
        add_to_dependencies=True,
    ):
        self.set_tables(input_table, output_table, output_table_key, is_dag_output_table)
        if sql_index:
            sql = self.ds_dag_manager.sql_creator.get_sql(
                sql_index=sql_index,
                input_table=self.input_table,
                op_kwargs=op_kwargs,
            )
            obj = self.ds_dag_manager.sql_creator.__class__
            self.add_sql_to_scripts(obj=obj, index=sql_index)

        if sql:
            limit = self.config_file.get_value_if_local(key='sql_row_limit', default_val=None)
            # Adds limit string to sql if local and local_config has limit which is not negative and there's no limit already in the sql
            if limit is not None and limit > -1 and 'limit' not in sql.lower():
                sql += f"\nlimit {limit}"

        with self.dag:
            if use_spark:
                cmd_args = ['--output_table', self.current_output_table]

                if sql:
                    cmd_args += ['--sql', sql]
                    entrypoint = FilePaths.create_table_from_sql
                else:
                    cmd_args += ['--input_table', self.input_table]

                if additional_args:
                    cmd_args += additional_args

                task = self.create_platyspark_operator(
                    task_id=task_id,
                    cmd_args=cmd_args,
                    lib_path=lib_path,
                    path_from_github_root=path_from_github_root,
                    entrypoint=entrypoint
                )

            else:
                self.md.scripts.append(f"The {task_id} SQL is run by a Trino Operator.")
                output_table = self.current_output_table
                tmp_output_table = f"{output_table}_tmp"
                sql = f"""
                    drop table if exists {tmp_output_table};
                    create table {tmp_output_table} as
                    {sql};
                    drop table if exists {output_table};
                    alter table {tmp_output_table} rename to {output_table};
                    """.replace('                    ', '')
                task = TrinoOperator(
                    task_id=task_id,
                    sql=sql
                )

            if sql:
                self.add_sql_to_sqls(sql=sql, task_id=task_id)

            if add_to_dependencies:
                self.add_to_dependencies_and_doc(task=task, description=description)
            elif description:
                self.md.add_task(task_id=task_id, description=description)

            return task

    def new_tasks_run_model(
            self,
            input_table=None,
            output_table=None,
            output_table_key=TmpTableKeys.MODEL_OUTPUT_TABLE,
            is_dag_output_table=False,
    ):
        self.set_tables(input_table, output_table, output_table_key, is_dag_output_table)
        with self.dag:
            run_model = PythonVirtualenvOperator(
                python_callable=trigger_batch_predict,
                task_id="run_model",
                op_kwargs={'input_table': self.input_table,
                           'output_table': self.current_output_table,
                           'model_name': self.model_name,
                           'timeout_minutes': 180
                           },
                do_xcom_push=True,
                requirements=['wixml-clients'],
            )
            self.add_to_dependencies_and_doc(task=run_model)

            wait = PythonSensor(task_id='wait_for_model',
                                op_kwargs={"job_id": f"{{{{ ti.xcom_pull(task_ids='{run_model.task_id}') }}}}",
                                           "job_type": "bp"},
                                python_callable=wait_on_job, poke_interval=2 * 60,
                                mode="reschedule")

            self.add_to_dependencies_and_doc(task=wait)

            cmd_args = ['--kernel_table', self.input_table,
                        '--output_table', self.current_output_table,
                        '--process_short_name', self.model_short_name,
                        '--validation_threshold', self.get_from_config_file_root(DagConfKeys.VALIDATION_THRESHOLD),
                        '--dropped_rows_table', self.dropped_rows_table]

            validate_results = self.create_platyspark_operator(
                task_id=f"validate_results",
                cmd_args=cmd_args,
                entrypoint=FilePaths.validate_script,
                retries=2
            )
            self.add_to_dependencies_and_doc(task=validate_results)

            return run_model, wait, validate_results

    def new_task_project_features(
            self,
            task_id='project_features',
            input_table=None,
            output_table=None,
            output_table_key=TmpTableKeys.MODEL_INPUT_TABLE,
            is_dag_output_table=False,
            features=None
    ):
        self.set_tables(input_table, output_table, output_table_key, is_dag_output_table)
        with self.dag:
            cmd_args = [
                '--input_table', self.input_table,
                '--output_table', self.current_output_table,
                '--features', features or self.get_from_dag_run(DagConfKeys.MODEL_SCHEMA),
            ]
            task = self.create_platyspark_operator(
                task_id=task_id,
                cmd_args=cmd_args,
                entrypoint=FilePaths.project_features_script
            )
            self.add_to_dependencies_and_doc(task=task)
            return task

    def new_task_join_result(
            self,
            task_id='join_result',
            prediction_table=None,
            input_table=None,
            output_table=None,
            output_table_key=None,
            is_dag_output_table=True,
            features=None
    ):
        self.set_tables(input_table, output_table, output_table_key, is_dag_output_table)
        with self.dag:
            cmd_args = [
                '--prediction_table', prediction_table or self.model_output_table,
                '--input_table', self.model_input_table_all_fields,
                '--output_table', self.current_output_table,
                '--features', features or self.get_from_dag_run(DagConfKeys.MODEL_SCHEMA),
            ]
            task = self.create_platyspark_operator(
                task_id=task_id,
                cmd_args=cmd_args,
                entrypoint=FilePaths.join_result_script
            )

            self.add_to_dependencies_and_doc(task=task)
            return task

    def new_tasks_store_results(
        self,
        table_to_store=None,  # By default, will use self.current_output_table
        append_to_history=False,
        update_wt=False,
        write_events=False,
        task_prefix='',
        history_table_key=DagConfKeys.HISTORY_TABLE,
        history_fields_key=None,
        wt_fields_key=None,
        execution_date=None,
        wt_add_date_updated=True,  # Will add date_updated to wt query if it doesn't exist (if it's already being sent this won't break anything)
        event_query_type=None,
        pool=None,
        add_to_doc=True
    ):
        with self.dag:
            output_ready_task = EmptyOperator(task_id='output_ready')
            self.add_to_dependencies_and_doc(task=output_ready_task)
            output_storing_kickoff_task = EmptyOperator(task_id='output_storing_kickoff')
            skip_storing_output_task = EmptyOperator(task_id='skip_storing_output')
            output_stored_task = EmptyOperator(task_id='output_stored', trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

            self.new_task_branch(
                task_id="check_if_store_output",
                op_kwargs=dict(condition=self.get_from_dag_run('store_to_history_wt_and_events', True)),
                true_task=output_storing_kickoff_task,
                false_task=skip_storing_output_task,
                true_label="Store output",
                false_label="Don't store output",
                true_task_description="Start storing output (empty task for branch operator)",
                false_task_description="Skip storing output (empty task for branch operator)",
                after_task=output_stored_task,
            )
            # The output_stored_task was created and set downstream of skip (good) and store (unnecessary, makes the graph confusing)
            # We'll remove it from downstream of the stored task (it will be downstream of stored's other downstream tasks)
            output_storing_kickoff_task.downstream_task_ids.remove(output_stored_task.task_id)
            self.md.tasks.pop()  # Also remove output_stored_task from documentation. We'll add it back later.

            self.prev_task = output_storing_kickoff_task

            tasks = []
            results_table = table_to_store or self.current_output_table
            history_table = self.get_history_table(key=history_table_key)
            if append_to_history:
                cmd_args = [
                    '--input_table', results_table,
                    '--output_table', history_table,
                    '--run_type', self.run_type or self.get_from_dag_run(DagConfKeys.RUN_TYPE)
                ]

                if execution_date:
                    cmd_args += ['--execution_date', execution_date]

                if history_fields_key:
                    cmd_args += ['--output_cols', self.get_from_dag_run(history_fields_key)]

                task_id = f'append_to_{history_table_key}'

                append_to_history_task = self.create_platyspark_operator(
                    task_id=task_id,
                    cmd_args=cmd_args,
                    entrypoint=FilePaths.append_to_history_script
                )

                append_to_history_task.max_active_tis_per_dag = 1
                if pool:
                    append_to_history_task.pool = f"{pool}_append_to_history"

                self.prev_task >> append_to_history_task >> output_stored_task
                tasks.append(append_to_history_task)

            if update_wt:
                cmd_args = [
                    '--delta_table', results_table,
                    '--wide_table', self.get_from_dag_run(DagConfKeys.WT),
                    '--date_column', self.get_from_dag_run(DagConfKeys.WT_DATE_COLUMN) or 'execution_date',
                ]
                if wt_fields_key:
                    cmd_args += ['--select_fields', self.get_from_dag_run(wt_fields_key)]

                if wt_add_date_updated:
                    cmd_args.append('--add_date_updated')

                update_wt = self.create_platyspark_operator(
                    task_id=f'{task_prefix}update_wt',
                    cmd_args=cmd_args,
                    entrypoint=FilePaths.update_wt_script
                )
                update_wt.max_active_tis_per_dag = 1
                if pool:
                    update_wt.pool = f"{pool}_update_wt"

                self.prev_task >> update_wt >> output_stored_task
                tasks.append(update_wt)

            if write_events:
                sql = self.ds_dag_manager.event_creator.get_sql(
                    event_query_type=event_query_type,
                    source_table=results_table,
                    execution_date="{{ ds }}",
                    limit=self.config_file.get_value_if_local(key='events_limit', default_val=None)
                )
                task_id = 'write_events'
                write_events = TrinoOperator(
                    task_id=task_id,
                    sql=sql,
                )
                obj = self.ds_dag_manager.event_creator.__class__
                self.add_sql_to_scripts(obj=obj, index=event_query_type, sql_type='Event SQL')

                self.add_sql_to_sqls(sql=sql, task_id=task_id)

                self.prev_task >> write_events >> output_stored_task
                tasks.append(write_events)

            tasks.append(output_stored_task)
            if add_to_doc:
                self.md.add_tasks(tasks=tasks)

            self.prev_task = output_stored_task
            return tasks

    def new_task_drop_temps(self):
        if self.tables_to_drop:
            with self.dag:
                task = TrinoOperator(
                    task_id='drop_temps',
                    sql='\n'.join([f"""drop table if exists {table};""" for table in self.tables_to_drop])
                )

            self.add_to_dependencies_and_doc(task=task)
            return task

    def new_task_write_dropped_rows(
            self,
            drop_step,
            input_table=None,
            output_table=None,
            add_to_doc=True
    ):
        """
        Creates a task that appends dropped rows to the dropped rows table
        @param drop_step: DropStep object
        @param input_table: By default this will be the input table of the previous task
        @param output_table: By default this will be the output table of the previous task
        @param add_to_doc: Whether to add the task to the documentation
        @return:
        """
        self.input_table = input_table or self.input_table
        self.current_output_table = output_table or self.current_output_table
        sql = f"""
            insert into {self.dropped_rows_table} 
            select msid, '{drop_step['name']}' as drop_step, {drop_step['priority']} as priority
            from {self.input_table}
            where msid not in (select msid from {self.current_output_table})
        """

        with self.dag:
            task = TrinoOperator(
                task_id=f"dropped_rows_{drop_step['name']}",
                sql=sql
            )
            self.add_to_dependencies_and_doc(task=task, description="Writes dropped rows to the dropped rows table.", add_to_doc=add_to_doc)
            return task

    def new_task_trigger_dag(self, dag_id, trigger_conf, add_to_dependencies=True):
        with self.dag:
            trigger_dag = TriggerDagRunOperator(
                trigger_dag_id=dag_id,
                task_id=f"trigger_{dag_id}",
                conf=trigger_conf,
                wait_for_completion=False,
            )
            if add_to_dependencies:
                self.add_to_dependencies_and_doc(task=trigger_dag)

            return trigger_dag

    def new_task_branch(
            self,
            true_task,
            false_task,
            true_task_description='',
            false_task_description='',
            true_label='',
            false_label='',
            after_task=None,  # if not passed, will create one
            task_id='branch',
            python_callable=None,
            op_kwargs=None,
            description='',
    ):
        if not python_callable:
            python_callable = branch_by_condition

        if op_kwargs is None:
            op_kwargs = {}

        op_kwargs['true_task_id'] = true_task.task_id
        op_kwargs['false_task_id'] = false_task.task_id
        branch_task = BranchPythonOperator(
            task_id=task_id,
            python_callable=python_callable,
            op_kwargs=op_kwargs
        )
        if not after_task:
            after_task = EmptyOperator(task_id=f'after_{task_id}', trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

        self.add_to_dependencies_and_doc(task=branch_task, description=description)
        self.md.add_task(task_id=true_task.task_id, description=true_task_description)
        self.md.add_task(task_id=false_task.task_id, description=false_task_description)

        self.md.add_task(task_id=after_task.task_id, description='Empty task following branch.')

        branch_task >> Label(true_label) >> true_task >> after_task
        branch_task >> Label(false_label) >> false_task >> after_task

        self.prev_task = after_task
        return branch_task, after_task

    def new_task_python_operator(
            self,
            task_id,
            python_callable,
            op_kwargs=None,
            description="",
            add_to_doc=True
    ):
        with self.dag:
            task = PythonOperator(
                task_id=task_id,
                python_callable=python_callable,
                op_kwargs=op_kwargs,
                provide_context=True
            )
            if add_to_doc:
                self.add_to_dependencies_and_doc(task=task, description=description)
                self.add_callable_to_scripts(task_id=task_id, func=python_callable)
            return task

    def new_task_sensor(
            self,
            task_id,
            python_callable,
            op_kwargs=None,
            poke_interval=60,
            timeout=60 * 60,
            description="",
            add_to_doc=True,
    ):
        with self.dag:
            task = PythonSensor(
                task_id=task_id,
                python_callable=python_callable,
                op_kwargs=op_kwargs,
                poke_interval=poke_interval,
                timeout=timeout,
            )
            if add_to_doc:
                self.add_to_dependencies_and_doc(task=task, description=description)
                self.add_callable_to_scripts(task_id=task_id, func=python_callable)
            return task

    def new_tasks_run_github_workflow(
            self,
            inputs,
            github_token=None,
            repository=None,
            workflow_id=None,
    ):
        github_token = github_token or self.get_from_config_file(DagConfKeys.GITHUB_TOKEN)
        repository = repository or self.get_from_config_file(DagConfKeys.GITHUB_WORKFLOW_REPO)
        workflow_id = workflow_id or self.get_from_config_file(DagConfKeys.WORKFLOW_ID)

        trigger_task = self.new_task_python_operator(
            task_id='trigger_workflow',
            description="Triggers the GitHub Actions workflow",
            python_callable=trigger_github_workflow,
            op_kwargs=dict(
                github_token=github_token,
                repository=repository,
                workflow_id=workflow_id,
                inputs=inputs
            ),
        )
        trigger_task.max_active_tis_per_dag = 1

        sensor_task = self.new_task_sensor(
            task_id='wait_for_workflow_completion',
            description="Waits for the GitHub Actions workflow to complete",
            python_callable=check_github_workflow_status,
            op_kwargs=dict(
                repository=repository,
                run_id="{{ task_instance.xcom_pull(task_ids='trigger_workflow') }}",
                github_token=github_token
            ),
            timeout=60 * 60,
            poke_interval=30,
        )

        return trigger_task, sensor_task


    def create_platyspark_operator(
            self,
            task_id,
            cmd_args,
            entrypoint,
            lib_path=FilePaths.lib_path,
            path_from_github_root=FilePaths.lib_path_from_github_root,
            retries=1
    ):
        task_string = task_id.replace('_', ' ').capitalize()
        script_type = "ds_dag's " if 'ds_dag' in str(path_from_github_root) else "custom script "
        link = self.ds_dag_manager.create_script_link(path_from_github_root=path_from_github_root, entrypoint=entrypoint)

        self.md.scripts.append(f"{task_string} is done by {script_type} {link}.")

        return PlatySparkRunAppOperator(
            application_location=LocalLocation(
                application_code=lib_path,
                entrypoint=lib_path / entrypoint),
            spark_conf={"spark.yarn.queue": "ds"},
            cmd_args=cmd_args,
            task_id=task_id,
            retries=retries,
            cluster_config=SharedClusterConfig(),
            dag=self.dag,
        )

    def get_from_config_file(self, key, default_val=None):
        return self.config_file.get_dag_value(dag_id=self.dag_id, key=key, default_val=default_val) or \
               self.get_from_config_file_root(key=key, default_val=default_val)

    def get_from_config_file_root(self, key, default_val=None):
        return self.config_file.get(key=key, default_val=default_val)

    def get_value_if_local(self, key, default_val=''):
        return self.config_file.get_value_if_local(key, default_val)

    def get_other_dag_value(self, dag_id, key, default_val=''):
        val = self.config_file.get_dag_value(dag_id, key, default_val)
        return val

    @staticmethod
    def get_from_dag_run(key, default_val=''):
        return f"{{{{ dag_run.conf.get('{key}', {default_val}) }}}}"

    @staticmethod
    def get_dag_run_conf():
        return "{{ dag_run.conf }}"

    @staticmethod
    def get_dag_start_date():
        return "{{ dag_run.start_date }}"

    @staticmethod
    def get_dag_param(param):
        return f"{{{{ dag_run.{param} }}}}"

    def add_callable_to_scripts(self, task_id, func):
        filename = f"{self.ds_dag_manager.file_root}/{func.__module__.replace('.', '/')}.py"
        code_location = f"{filename}#L{func.__code__.co_firstlineno}"
        script_type = "ds_dag's " if 'ds_dag' in str(filename) else "custom script "
        self.md.scripts.append(f"**{task_id}** is performed by {script_type} [{func.__name__}]({code_location}).")

    def add_sql_to_scripts(self, obj, index, sql_type='SQL'):
        filename = f"{self.ds_dag_manager.file_root}{obj.__module__.replace('.', '/')}.py"
        self.md.scripts.append(f"The {sql_type} is created by [{obj.__name__}]({filename}) using the index {index}.")

    def add_sql_to_sqls(self, sql, task_id):
        self.md.sqls.append(f"<details><summary>{task_id} sql</summary>\n\n```sql\n{dedent(sql)}\n```\n\n</details>")

    def get_default_history_table_name(self, short_name=""):
        if not short_name:
            short_name = self.get_from_config_file(DagConfKeys.SHORT_NAME)

        table_name = f"{self.get_from_config_file(DagConfKeys.NORMAL_PREFIX)}{short_name}_history"
        return table_name


def branch_by_condition(condition, true_task_id, false_task_id):
    """
    Branch to true task if boolean True or String value that isn't False
    Otherwise branch to false task
    """
    if condition:
        if not (isinstance(condition, str) and condition.lower() == "false"):
            return true_task_id

    return false_task_id
