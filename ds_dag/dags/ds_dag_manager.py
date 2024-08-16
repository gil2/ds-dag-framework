# Shared ds files
from ds_dag.dags.ds_config_file_class import DSConfigFile
from ds_dag.dags.dsdag import DSDag
from ds_dag.dags.event_creator import EventCreator
from ds_dag.dags.local_config import local_config as default_local_config
from ds_dag.dags.orchestrator import Orchestrator
from ds_dag.dags.sql_creator import SQLCreator


class DSDagManager:
    def __init__(self,
                 config: dict = None,
                 sql_creator: SQLCreator = None,
                 event_creator: EventCreator = None,
                 local_config: dict = default_local_config,
                 ):
        self.config_file = DSConfigFile(local_config=local_config, config=config)
        self.sql_creator = sql_creator
        self.event_creator = event_creator
        self.config_file_link = ""
        self.created_by = ""
        self.created_dags_dict = {}
        self.file_root = "https://github.com/wix-private/wix-data-dev/blob/master"

    def create_dag(self, dag_id):
        ds_dag = DSDag(dag_id=dag_id, ds_dag_manager=self)
        ds_dag.create_dag()
        self.created_dags_dict[dag_id] = ds_dag
        return ds_dag

    def create_orchestrators(self):
        run_types = self.config_file.get_orchestrators()
        for run_type in run_types.keys():
            orchestrator = self.create_orchestrator(run_type=run_type)
            self.created_dags_dict[run_type] = orchestrator

        return self.created_dags_dict

    def create_orchestrator(self, run_type):
        orchestrator = Orchestrator(run_type=run_type, ds_dag_manager=self)
        orchestrator.create_dag()
        return orchestrator

    def create_dags(self, dag_ids):
        self.created_dags_dict = {dag_id: self.create_dag(dag_id) for dag_id in dag_ids}
        return self.created_dags_dict

    def finish_creating_dags(self):
        for dag in self.created_dags_dict.values():
            dag.new_task_drop_temps()
            dag.md.create_doc_md()

    def get_ds_dag(self, dag_id):
        return self.created_dags_dict.get(dag_id)

    def create_script_link(self, path_from_github_root: str, entrypoint: str):
        """
        Creates a link to a script specified by path
        Converts the actual file link to the GitHub file link, and creates a link
        :param path_from_github_root: the path from the root of the GitHub repo
        :param entrypoint: the entrypoint file
        @return: link string, in the form of [short_name](link)
        """
        github_link = f"{self.file_root}/{path_from_github_root}/{entrypoint}"

        short_name = entrypoint.split('/')[-1]
        link = f"[{short_name}]({github_link})"
        return link
