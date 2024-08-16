import sys
from pathlib import Path

from jinja2 import Template

from ds_dag.dags.dag_constants import DagConfKeys


class DSDocCreator:
    def __init__(self, ds_dag, doc_file='doc.md'):
        self.ds_dag = ds_dag
        self.ds_dag_root = f"{self.ds_dag.ds_dag_manager.file_root}/ds/ds_dag/"

        self.template = Template(self.get_file_contents(doc_file))
        self.local_config_content = self.ds_dag.ds_dag_manager.config_file.local_config

        self.title = self.ds_dag.dag_id.replace('_', ' ').title()
        self.essence = self.ds_dag.get_from_config_file(DagConfKeys.ESSENCE, '')
        self.tasks = []
        self.scripts = []
        self.essence_extra = []
        self.sqls = []
        self.created_by = self.ds_dag.ds_dag_manager.created_by
        self.more_info = ""
        self.event_info = ""

        # For orchestrators
        self.airflow_dags_path = "https://bo.wix.com/airflow-prod/dags"
        self.internal_dags = []
        self.history_tables = []
        self.wide_tables = []
        self.events = []
        self.special_backfill_instructions = ""
        self.wide_tables_extra = ""

        if self.ds_dag.ds_dag_manager.config_file_link:
            self.scripts.append(f"The configuration is in {self.ds_dag.ds_dag_manager.config_file_link}.")

    @staticmethod
    def get_file_contents(filename):
        with open(Path(__file__).parent.parent / 'docs' / filename, 'r') as file:
            return file.read()

    def create_dag_link(self, dag_id):
        return f"[{dag_id}]({self.airflow_dags_path}/{dag_id})"

    def add_internal_dag(self, dag_id):
        self.internal_dags.append(f"1. **{self.create_dag_link(dag_id)}**: {self.ds_dag.config_file.get_dag_value(dag_id, DagConfKeys.ESSENCE)}")

    def add_to_wide_tables(self, dag_id):
        self.wide_tables.append(f"* **{self.ds_dag.config_file.get_dag_value(dag_id, DagConfKeys.WT)}** (updated by {self.create_dag_link(dag_id)})")

    def add_tasks(self, tasks):
        for task in tasks:
            self.add_task(task_id=task.task_id)

    def add_task(self, task_id, description=""):
        match task_id:
            case "create_input_table":
                task_string = "Creates the input table."
            case "run_model":
                task_string = f"Runs the **{self.ds_dag.get_from_config_file(DagConfKeys.MODEL, '')}** model."
            case "wait_for_model":
                task_string = "Waits for the model to finish"
            case "validate_results":
                task_string = f"Validates that the model processed at least {self.ds_dag.get_from_config_file(DagConfKeys.VALIDATION_THRESHOLD, '')}% of the rows."
            case "output_ready":
                task_string = "Empty task. The orchestrator waits for this to know it can continue to subsequent tasks."
            case "check_if_store_output":
                task_string = "Branch operator checking if user chose to not store the output."
            case "append_to_history_table":
                task_string = f"Adds this output to **{self.ds_dag.get_from_config_file(DagConfKeys.HISTORY_TABLE) or self.ds_dag.get_default_history_table_name()}**."
                self.essence_extra.append(f"* {task_string}")
            case "update_wt":
                task_string = f"Updates wide table **{self.ds_dag.get_from_config_file(DagConfKeys.WT)}**."
                self.essence_extra.append(f"* {task_string}")
            case "write_events":
                task_string = self.event_info
                self.essence_extra.append(f"* {task_string}")
            case "output_stored":
                task_string = "Empty task, indicates that the above tasks completed, and the output is stored (if store_output is true)."
            case "drop_temps":
                task_string = "Drops the temporary tables created by and for this dag. " \
                              "For the Orchestrator, this also includes dropping the output tables of the internal dags it calls."
            case "project_features":
                task_string = "Selects only a subset of the input table's fields for the model."
            case "join_result":
                task_string = "Joins the results of the model with the other fields from the input table."
            case _:
                task_string = ""

        self.tasks.append(f"1. **{task_id}**: {description or task_string}")

    def create_doc_md(self):
        essence_extra_string = ""
        if self.essence_extra:
            essence_extra_string += "\n\nAlso:\n\n" + "\n".join(self.essence_extra)

        content = self.template.render(
            title=self.title,
            local_config=self.local_config_content,
            essence=self.essence,
            essence_extra=essence_extra_string,
            tasks="\n\n".join(self.tasks),
            scripts="\n\n".join(self.scripts),
            ds_dag_root=self.ds_dag_root,
            created_by=self.created_by,
            more_info=self.more_info,
            config_link=self.ds_dag.ds_dag_manager.config_file_link,
            internal_dags="\n".join(self.internal_dags),
            history_tables="\n".join(self.history_tables),
            wide_tables="\n".join(self.wide_tables) + self.wide_tables_extra,
            special_backfill_instructions=self.special_backfill_instructions,
            event_info=self.event_info,
        )

        if self.sqls:
            content += "\n\n### SQLs:\n\n" + "\n".join(self.sqls)

        self.ds_dag.dag.doc_md = content

        doc_md_file = Path(__file__).parent / f"{self.ds_dag.dag_id}.md"
        # self.docs_config = self.ds_dag.get_from_config_file('docs_config', {}).get(self.ds_dag.dag_id, {})

        if sys.gettrace() is not None:
            with open(doc_md_file, 'w') as file:
                file.write(content)

        return content

