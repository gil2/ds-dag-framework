"""
This file is at the bottom of all dependencies; it does not import anything from the rest of the project.

"""
from pathlib import Path


class TmpTableKeys:
    INPUT_TABLE = "input_table"
    MODEL_INPUT_TABLE_ALL_FIELDS = "model_input_table_all_fields"
    MODEL_INPUT_TABLE = "model_input_table"
    MODEL_OUTPUT_TABLE = "model_output_table"
    OUTPUT_TABLE = "output_table"
    DROPPED_ROWS_TABLE = "dropped_rows_table"


class DagConfKeys:
    DAGS = "dags"
    ORCHESTRATORS = "orchestrators"
    SCHEDULE = "schedule"
    TAGS = "tags"
    PARAMS = "params"

    RUN_TYPE = "run_type"
    RUN_TIME_AND_TYPE = "run_time_and_type"
    INPUT_DAG = "input_dag"
    INPUT_TABLE = "input_table"
    OUTPUT_TABLE = "output_table"

    MODEL = "model"
    MODEL_SCHEMA = "model_schema"
    SHORT_NAME = "short_name"

    WT = "wt"
    WT_DATE_COLUMN = "wt_date_column"
    WT_FIELDS = "wt_fields"

    HISTORY_TABLE = "history_table"
    HISTORY_FIELDS = "history_fields"

    EVENT_QUERY_TYPE = "event_query_type"

    SQL_INDEX = "sql_index"
    LANGUAGES = "languages"

    FILTER_LOG_TABLE = "filter_log_table"

    DROPPED_ROWS_PREFIX = "dropped_rows_prefix"
    DROPPED_ROWS_TABLE = "dropped_rows_table"

    TABLE_TO_WAIT_FOR = "table_to_wait_for"
    DONT_APPLY_PREFIX = "dont_apply_prefix"
    NORMAL_PREFIX = "normal_prefix"

    CREATE_TABLE_ENTRYPOINT = "create_table_entrypoint"

    SLACK_CHANNEL = "slack_channel"
    EMAILS = "emails"
    VALIDATION_THRESHOLD = "validation_threshold"

    ESSENCE = "essence"

    GITHUB_TOKEN = "github_token"
    GITHUB_WORKFLOW_REPO = "github_workflow_repo"
    WORKFLOW_ID = "workflow_id"
    ECR_REPOSITORY_NAME = "ecr_repository_name"

    CONFIG_FILE_NAME = "config_file_name"
    AWS_CONN_ID = "aws_conn_id"
    KAFKA_TOPIC = "kafka_topic"
    KAFKA_CLUSTER = "kafka_cluster"


class FilePaths:
    lib_path_from_github_root = "ds/ds_dag/lib"
    lib_path = Path(__file__).resolve().parents[1] / "lib"
    create_table_from_sql = "create_table.py"
    project_features_script = "project_features.py"
    validate_script = "validate_results.py"
    join_result_script = "join_result.py"
    update_wt_script = "merge_to_wt.py"
    append_to_history_script = "append_to_history.py"
