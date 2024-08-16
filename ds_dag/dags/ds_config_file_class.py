from airflow.configuration import conf

from ds_dag.dags.dag_constants import DagConfKeys


class DSConfigFile:
    def __init__(self, local_config, config=None):
        self.local_config = local_config
        self.config = config if config else {}

        ENV_NAME = conf.get(section="webserver", key="instance_name", fallback="default_airflow_env"). \
            replace(" ", "_").strip('\"')

        self.is_local = (ENV_NAME != "Production")

        self.prefix = self.get_value_if_local('prefix')

        self.run_time_and_type = "{{ ts_nodash.replace('T','') }}_{{ dag_run.conf.get('run_type', params.run_type) }}"

    def get_tmp_table_name(self, dag_id, key, run_time_and_type):
        table_name = f"prod.ds.{dag_id}"
        if key:
            table_name += f"_{key}"

        table_name += f"_{run_time_and_type}"

        table_name = self.apply_prefix(table_name)
        return table_name

    def apply_prefix(self, val):
        exclusions = self.config.get(DagConfKeys.DONT_APPLY_PREFIX)
        if self.prefix and isinstance(val, str):
            if not (exclusions and (val in exclusions)):
                if self.prefix not in val:
                    val = val.replace("sandbox.", self.prefix)
                    val = val.replace("prod.", self.prefix)

        return val

    def get(self, key, default_val=''):
        val = self.config.get(key, default_val)
        val = self.get_value_if_local(key, val)
        return self.apply_prefix(val)

    def get_dags(self):
        return self.get('dags', {})

    def get_orchestrators(self):
        return self.get('orchestrators').get('run_types')

    def get_orchestrator_defaults(self):
        return self.get('orchestrators').get('defaults')

    def get_orchestrator(self, run_type):
        return self.get_orchestrators().get(run_type)

    def get_orchestrator_value(self, run_type, key, default_val=''):
        val = self.get_orchestrator(run_type).get(key, default_val)
        if not val:
            val = self.get_orchestrator_defaults().get(key, default_val)

        if not val:
            val = self.get(key, default_val)

        val = self.get_value_if_local(key, val)
        return self.apply_prefix(val)

    def get_dag_conf(self, dag_id):
        if dag_id in self.get_dags().keys():
            return self.get_dags().get(dag_id)
        else:
            return {}

    def get_dag_value(self, dag_id, key, default_val=''):
        dag_conf = self.get_dag_conf(dag_id)
        if key in dag_conf.keys():
            val = dag_conf.get(key, default_val)
        else:
            val = self.get(key, default_val)

        val = self.get_value_if_local(key, val)
        return self.apply_prefix(val)

    def get_value_if_local(self, key, default_val=''):
        if self.is_local:
            val = self.local_config.get(key, default_val)
            return val
        else:
            return default_val
