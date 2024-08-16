from datetime import timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.sensors.python import PythonSensor
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from wix_trino_client.trino_connection import WixTrinoConnection
from wixflow.operators.trino import TrinoOperator
# from wixflow.sensors.dds_sensor import DdsSensor, Dependency, SourceKind


from ds_dag.dags.dag_constants import DagConfKeys
from ds_dag.dags.dsdag import DSDag


class Orchestrator(DSDag):
    def __init__(self, run_type, ds_dag_manager):
        base_name = ds_dag_manager.config_file.get('orchestrators').get('name')
        super().__init__(
            dag_id=f"{base_name}_{run_type}_orchestrator",
            ds_dag_manager=ds_dag_manager,
            run_type=run_type,
            doc_file='orchestrator_doc.md'
        )
        self.retries = 0
        self.slack_channel = self.get_from_config_file('slack_channel') or f"site-class-{run_type}-alerts"
        self.schedule = self.get_from_config_file(DagConfKeys.SCHEDULE, None)
        self.execution_timeout_hours = self.get_from_config_file('execution_timeout_hours')
        self.dagrun_timeout_hours = self.get_from_config_file('dagrun_timeout_hours')
        self.triggered_dag_timeout_hours = self.get_from_config_file('triggered_dag_timeout_hours')
        self.triggered_dag_execution_date = self.get_from_config_file('triggered_dag_execution_date')
        self.revisions_ds = self.get_from_config_file('revisions_ds')
        self.execution_date = self.get_from_config_file('execution_date')
        self.execution_delta = self.get_from_config_file('execution_delta')
        self.table_to_wait_for = self.get_from_config_file(DagConfKeys.TABLE_TO_WAIT_FOR)
        self.wait_for_all_dag_run_complete = self.get_from_config_file('wait_for_all_dag_run_complete')
        self.dropped_rows_prefix = self.get_from_config_file(DagConfKeys.DROPPED_ROWS_PREFIX)
        if self.dropped_rows_prefix:
            self.dropped_rows_table = self.config_file.get_tmp_table_name(
                self.dropped_rows_prefix,
                'dropped_rows',
                self.run_time_and_type
            )
            self.tables_to_drop.append(self.dropped_rows_table)
        else:
            self.dropped_rows_table = None

        self.store_results = True
        self.input_table = None
        self.prev_task = None

    @property
    def run_time_and_type(self):
        return "{{ ts_nodash.replace('T','') }}_" + self.run_type

    def get_from_config_file(self, key, default_value=None):
        return self.config_file.get_orchestrator_value(self.run_type, key, default_value)

    def is_backfill(self):
        return self.run_type == "backfill"

    def _child_create_dag(self):
        self.default_args['execution_timeout'] = timedelta(hours=self.execution_timeout_hours)
        self.default_args['dagrun_timeout'] = timedelta(hours=self.dagrun_timeout_hours)
        self.default_args['retries'] = self.retries

        dags = self.config_file.get_dags()
        # Add history table to default dags
        for dag_id in dags.keys():
            if DagConfKeys.HISTORY_TABLE not in dags[dag_id].keys():
                dags[dag_id][DagConfKeys.HISTORY_TABLE] = self.get_default_history_table_name(short_name=dags[dag_id][DagConfKeys.SHORT_NAME])

            # Add to documentation
            self.md.add_internal_dag(dag_id=dag_id)
            self.md.history_tables.append(f"* **{self.config_file.apply_prefix(dags[dag_id][DagConfKeys.HISTORY_TABLE])}**")
            if DagConfKeys.WT in dags[dag_id].keys():
                self.md.add_to_wide_tables(dag_id=dag_id)

        self.dag = DAG(
            dag_id=self.dag_id,
            default_args=self.default_args,
            concurrency=1,
            schedule=self.schedule,
            catchup=False,
            render_template_as_native_obj=True,
            params={
                "run_type": self.run_type,
                "store_to_history_wt_and_events": self.store_results,
                DagConfKeys.DAGS: dags
            }
        )

    def _child_setup_created_dag(self):
        with self.dag as dag:
            if self.schedule is not None:
                wait_for_dependencies = PythonSensor(
                    task_id='wait_for_dependencies',
                    python_callable=wait_on_snapshot,
                    op_kwargs=dict(
                        ds=self.revisions_ds,
                        input_table=self.table_to_wait_for
                    ),
                    retries=5,
                    poke_interval=60 * 5,
                    retry_delay=3,
                    timeout=60 * 60 * 24,
                    mode='reschedule',
                    dag=dag)

                self.add_to_dependencies_and_doc(
                    task=wait_for_dependencies,
                    description=f"Waits for **{self.get_from_config_file(DagConfKeys.TABLE_TO_WAIT_FOR)}** to be ready."
                )

            if self.dropped_rows_table:
                create_dropped_rows_table = TrinoOperator(
                    task_id='create_dropped_rows_table',
                    sql=dedent(f"""
                        create table if not exists {self.dropped_rows_table} (
                            msid varchar,
                            drop_step varchar,
                            priority double
                        )
                    """)
                )
                self.add_to_dependencies_and_doc(
                    create_dropped_rows_table,
                    description="Creates this run's temporary table of sites dropped by this pipeline."
                )

            self.md.add_task(task_id="Task Groups Triggering Internal Dags", description="See previous sections.")
            for dag_id in self.config_file.get_dags().keys():
                self.create_dag_group_tasks(dag_id=dag_id)

            if self.dropped_rows_table:
                self.history_table = self.dropped_rows_table.replace(self.run_time_and_type, 'history')
                self.new_tasks_store_results(
                    table_to_store=self.dropped_rows_table,
                    append_to_history=True,
                    execution_date=self.execution_date,
                    pool=f"{self.dropped_rows_prefix}_dropped_rows",
                    add_to_doc=False
                )
                self.md.add_task(task_id="append_to_history", description=f"Adds dropped rows to **{self.history_table}**.")
                self.md.add_task(task_id="output_stored", description=f"Finished adding dropped rows to **{self.history_table}**.")

            if self.wait_for_all_dag_run_complete:
                with TaskGroup(group_id="wait_for_all_dag_run_complete") as wait_for_all_dag_run_complete:
                    wait_for_tasks = {}
                    for dag_id in self.config_file.get_dags().keys():
                        task_id = f"wait_for_{dag_id}_dag_run_complete"
                        wait_for_tasks[task_id] = ExternalTaskSensor(
                            task_id=task_id,
                            external_dag_id=dag_id,
                            external_task_id=None,
                            mode='reschedule',
                            timeout=60 * 60 * self.triggered_dag_timeout_hours,
                            execution_delta=self.execution_delta,
                            failed_states=['failed'],
                            retries=3
                        )

                self.add_to_dependencies_and_doc(
                    wait_for_all_dag_run_complete,
                    description="Waits for all the internal dags to finish."
                )

    def create_dag_group_tasks(self, dag_id):
        with TaskGroup(group_id=f'{dag_id}_dag'):
            trigger_dag = TriggerDagRunOperator(
                task_id=f"trigger_{dag_id}_dag",
                trigger_dag_id=dag_id,
                execution_date=self.triggered_dag_execution_date,
                reset_dag_run=True,
                conf=f"{{{{ ti.xcom_pull(key='conf_{dag_id}') }}}}")

            wait_for_dag = ExternalTaskSensor(
                task_id=f"wait_for_{dag_id}",
                external_dag_id=dag_id,
                external_task_id='output_ready',
                mode='reschedule',
                timeout=60 * 60 * self.triggered_dag_timeout_hours,
                execution_delta=self.execution_delta,
                failed_states=['failed'],
                retries=3
            )

            enrich_kwargs = dict(
                dag_id=dag_id,
                conf=self.config_file.get_dag_conf(dag_id=dag_id),
                user_conf=self.get_from_dag_run('dags'),
                run_time_and_type=self.run_time_and_type,
                dropped_rows_table=self.dropped_rows_table,
                run_type=self.run_type,
                store_results=self.get_from_dag_run('store_to_history_wt_and_events', True),
                trigger_dag_task_id=None,
                skip_dummy_task_id=None,
                next_execution_date="{{ next_ds }}",
            )

            if self.is_backfill():
                skip_dummy = EmptyOperator(task_id=f"dummy_{dag_id}",
                                           trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

                enrich_kwargs.update({
                    'trigger_dag_task_id': trigger_dag.task_id,
                    'skip_dummy_task_id': skip_dummy.task_id,
                })

                check_if_should_run_and_enrich = BranchPythonOperator(
                    task_id=f"check_if_run_{dag_id}",
                    python_callable=self.enrich_dag_input,
                    op_kwargs=enrich_kwargs
                )
                self.add_to_dependencies_and_doc(check_if_should_run_and_enrich, add_to_doc=False)
                check_if_should_run_and_enrich >> skip_dummy
            else:
                enrich_dag_input_task = PythonOperator(
                    task_id=f"enrich_dag_input_{dag_id}",
                    python_callable=self.enrich_dag_input,
                    op_kwargs=enrich_kwargs
                )
                self.add_to_dependencies_and_doc(enrich_dag_input_task, add_to_doc=False)

            self.add_to_dependencies_and_doc(trigger_dag, add_to_doc=False)
            self.add_to_dependencies_and_doc(wait_for_dag, add_to_doc=False)

            output_table = self.get_output_table(dag_id=dag_id)
            if self.input_table and self.dropped_rows_table:
                drop_step = dict(name=dag_id, priority=2)
                self.new_task_write_dropped_rows(
                    drop_step=drop_step,
                    input_table=self.input_table,
                    output_table=output_table,
                    add_to_doc=False
                )

            self.tables_to_drop.append(output_table)
            self.input_table = output_table
            if self.is_backfill():
                self.add_to_dependencies_and_doc(skip_dummy, add_to_doc=False)

    def enrich_dag_input(
            self, ti, dag_id, conf, user_conf, run_time_and_type, next_execution_date, run_type,
            store_results, dropped_rows_table, trigger_dag_task_id, skip_dummy_task_id, **kwargs):
        """
        Enrich conf for the triggered_dag
        Daily & monthly orchestrators call this from a PythonOperator
        Backfill calls this from a BranchPythonOperator, and also checks whether dag should run, returning either trigger_dag_task_id or skip_dummy_task_id
        For backfill, first tries to enrich from user_conf

        Initially the conf is from the site_class_config file for this dag
        user_conf is the dags section the user entered in Trigger with Conf, for backfill
        If there is a user_conf then:
        * If this dag isn't in that section, don't run this dag
        * If this dag is in that section, use any values the user provided in that section
        """
        # user_conf_dag is the conf for this dag manually entered in Trigger with Conf
        # For daily (and other times when there is no user_conf), this will be an empty dict
        user_conf_dag = {}
        if user_conf:
            # If the dag_id isn't there, skip it
            if dag_id not in user_conf.keys():
                return skip_dummy_task_id

            # If the dag_id is there, even as an empty dict, use it
            user_conf_dag = user_conf.get(dag_id)

        conf["run_type"] = run_type
        conf["run_time_and_type"] = run_time_and_type
        conf["is_backfill"] = run_type == 'backfill'
        conf["store_to_history_wt_and_events"] = store_results
        if self.dropped_rows_table:
            conf["dropped_rows_table"] = dropped_rows_table

        if not self.is_backfill():
            conf["next_execution_date"] = next_execution_date

        # Change any values if necessary
        for key in conf.keys():
            # If it's in the user_conf_dag, use that value
            if key in user_conf_dag.keys():
                conf[key] = user_conf_dag[key]
            else:
                conf[key] = self.config_file.apply_prefix(conf[key])

        if DagConfKeys.HISTORY_TABLE not in conf.keys():
            history_table = self.get_default_history_table_name(short_name=conf['short_name'])
            conf[DagConfKeys.HISTORY_TABLE] = self.config_file.apply_prefix(history_table)

        ti.xcom_push(key=f"conf_{dag_id}", value=conf)
        return trigger_dag_task_id

    @staticmethod
    def get_from_dag_run(key, default_val=''):
        return f"{{{{ params.{key} }}}}"


# Should change or replace with something more general. Like Dependencies
def wait_on_snapshot(ti, ds, input_table, **kwargs):
    tc = WixTrinoConnection(username='wix')
    table = input_table.split('.')
    query = f"""select max(from_unixtime_ntz(revision_date_created/1000.0)) from {table[0]}.{table[1]}.{table[2]}
                where partition_date = date '{ds}'  -- + interval '7' day
                and date_trunc('day', from_unixtime_ntz(revision_date_created / 1000)) = timestamp '{ds}'"""
    ts = tc.execute_sql(query)[0][0]
    if ts:
        ti.xcom_push(key='max_ts', value=ts)
        return True
    return False
