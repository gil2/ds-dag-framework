# DS Dag Framework

The DS Dag Framework is Wix Data Science's framework for creating Airflow dags.

It is particularly useful for the following cases:
* Internal dags called by an orchestrator dag
* Dags that run a model (or some other long external task such as Feature Store Dataset Generation), wait for it to finish, validate results, store the output in history and wide tables, and create events.

## DS Dag Classes & Files

The dags directory has the following classes and files:

* [DSDagManager](https://github.com/wix-private/wix-data-dev/blob/master/ds/ds_dag/dags/ds_dag_manager.py) is a controller that creates the dags, and holds the SQL, Events and Doc creators, along with the config file and list of dags it created.
* [DSDag](https://github.com/wix-private/wix-data-dev/blob/master/ds/ds_dag/dags/dsdag.py) manages each dag. Use it to create the dag, its tasks, and its documentation.
* [Orchestrator](https://github.com/wix-private/wix-data-dev/blob/master/ds/ds_dag/dags/orchestrator.py) manages each orchestrator.
* [DocCreator](https://github.com/wix-private/wix-data-dev/blob/master/ds/ds_dag/dags/doc_creator.py) creates the documentation.
* [doc.md](https://github.com/wix-private/wix-data-dev/blob/master/ds/ds_dag/dags/doc.md) has the template of the documentation.
* [orchestrator_doc.md](https://github.com/wix-private/wix-data-dev/blob/master/ds/ds_dag/dags/orchestrator_doc.md) will have the template of the orchestrator's documentation. Currently it actually has the Site Classification orchestrators' documentation, but that will be extracted from this template.
* [SQLCreator](https://github.com/wix-private/wix-data-dev/blob/master/ds/ds_dag/dags/sql_creator.py) is the base class for SQL creation.
* [EventCreator](https://github.com/wix-private/wix-data-dev/blob/master/ds/ds_dag/dags/event_creator.py) is the base class for writing events.
* [Local Config](https://github.com/wix-private/wix-data-dev/blob/master/ds/ds_dag/dags/local_config.py) has settings that will overwrite normal settings when running locally. Specifically will (under certain circumstances) replace "prod." with your chosen prefix, and will limit the number of rows returned by some sql queries to your set limit.

The lib directory has:
* [append_to_history.py](https://github.com/wix-private/wix-data-dev/blob/master/ds/ds_dag/lib/append_to_history.py) appends an input (delta) table to an output (history) table. It partitions by execution_date. If output_cols is sent, only outputs those fields (plus the following). Also sends run_type & execution_date (as date) if sent. 
* [create_table.py](https://github.com/wix-private/wix-data-dev/blob/master/ds/ds_dag/lib/create_table.py) creates output_table from sql, using spark.
* [join_result.py](https://github.com/wix-private/wix-data-dev/blob/master/ds/ds_dag/lib/join_result.py) left joins (on msid) the prediction_table (minus features) into the input_table to create the output_table.
* [merge_to_wt.py](https://github.com/wix-private/wix-data-dev/blob/master/ds/ds_dag/lib/merge_to_wt.py) merges select_fields from delta_table into wide_table on join_fields (default is msid).
* [project_features.py](https://github.com/wix-private/wix-data-dev/blob/master/ds/ds_dag/lib/project_features.py) creates output_table with features from input_table.
* [validate_results.py](https://github.com/wix-private/wix-data-dev/blob/master/ds/ds_dag/lib/validate_results.py) fails if less than validation_threshold (default=99) percent of the rows in kernel_table are in output_table. If it succeeds, writes any dropped rows to dropped_rows_table, with priority 1 and drop_step f'model_{process_short_name}'. 

## Self-Updating, Semi-Automatic Documentation
As DSDag creates the Dag, it also creates the documentation that will appear in the DAG Docs section. See the DAG Docs section of https://bo.wix.com/airflow-prod/dags/sc_embeddings/grid for an example.

The documentation is created by the [doc_creator.py](https://github.com/wix-private/wix-data-dev/blob/master/ds/ds_dag/dags/doc_creator.py).

It starts by reading a template: 
* [doc.md template](https://github.com/wix-private/wix-data-dev/blob/master/ds/ds_dag/dags/doc.md) for normal dags 
* [orchestrator_doc.md template](https://github.com/wix-private/wix-data-dev/blob/master/ds/ds_dag/dags/orchestrator_doc.md) for orchestrators.

It reads the "essence" key from the dag's configuration of the config file.

It creates the tasks list, libraries and scripts used, and sql sources sections as it creates the tasks.

When you create the tasks from your code, you can override the task descriptions.

You can also add content to other sections when creating your dag.

## How to use

To use the DS Dag Framework:
1. Create your config object.
2. Create a DS Dag Manager.
3. Use that to create your DS Dags
4. Create tasks in your DS Dags.
5. Call the DS Dag Manager's finish_creating_dags method.

For example:
```python
# Create Airflow dags using the DS Dag Framework
from ds_dag.dags.dag_constants import DagConfKeys
from ds_dag.dags.ds_dag_manager import DSDagManager

# Create your config object (for big projects you should probably do this in a separate file)
my_config = {
    DagConfKeys.TAGS: ['my-tags'],
    DagConfKeys.SLACK_CHANNEL: "my-slack-channel-alerts",
    DagConfKeys.EMAILS: ['my_email@wix.com', 'other_mail@wix.com'],
    DagConfKeys.SCHEDULE: None,
    DagConfKeys.DAGS: {
        'my_dag_1': {
            DagConfKeys.ESSENCE: "Calls the classification model.",
            DagConfKeys.SHORT_NAME: "my_dag_1",
            DagConfKeys.MODEL: "my_model_1",
        },
        'my_dag_2': {
            DagConfKeys.ESSENCE: "Calls the other classification model.",
            DagConfKeys.SHORT_NAME: "my_dag_2",
            DagConfKeys.MODEL: "my_model_2",
            DagConfKeys.WT: "my_wide_table",
        },
    }
}

# Create a DS Dag Manager
ds_dag_manager = DSDagManager(config=my_config)

# Create your dags, and add tasks to them
dag = ds_dag_manager.create_dag('my_dag_id_1')
dag.new_task_create_table(sql='select msid, ts from my_table')
dag.new_tasks_run_model()
dag.new_tasks_store_results(append_to_history=True, update_wt=False)

dag = ds_dag_manager.create_dag('my_dag_id_2')
dag.new_task_create_table(sql='select msid, ts from my_table_2')
dag.new_tasks_run_model()
dag.new_tasks_store_results(append_to_history=True, update_wt=True)

# Call finish_creating_dags to finish creating the dags & documentation
ds_dag_manager.finish_creating_dags()
```

The above code will create two dags that each:
1. Create a table from an sql
2. Send that table as an input table to a model, trigger the model, wait for it to return, and validate that at least a certain percent (by default 99%) of the rows were returned by the model.
3. Stores the results to a history table, and / or a wide table.
4. Create the documentation for the dags


## Orchestrators
Orchestrators are dags that run a pipeline of other dags.

To create an orchestrator, call ds_dag_manager's create_orchestrator() instead of its create_dag().

The orchestrator will then build a pipeline of the dags in the dags section of the config object.

To have one dag use another dag's output as its input, set its DagConfKeys.INPUT_DAG parameter.

Put the orchestrators' configurations in section parallel to the dags.

Here's the code from the previous example, but now with an orchestrator that will run the dags at 3:15 UTC every morning, using the first dag's output as the second dag's input.

```python
# Create Airflow dags using the DS Dag Framework
from ds_dag.dags.dag_constants import DagConfKeys
from ds_dag.dags.ds_dag_manager import DSDagManager

# Create your config object (for big projects you should probably do this in a separate file)
my_config = {
    DagConfKeys.TAGS: ['my-tags'],
    DagConfKeys.SLACK_CHANNEL: "my-slack-channel-alerts",
    DagConfKeys.EMAILS: ['my_email@wix.com', 'other_mail@wix.com'],
    DagConfKeys.SCHEDULE: None,
    DagConfKeys.ORCHESTRATORS: {
        'daily': {
            DagConfKeys.SCHEDULE: "15 3 * * *"
        }
    },
    DagConfKeys.DAGS: {
        'my_dag_1': {
            DagConfKeys.ESSENCE: "Calls the classification model.",
            DagConfKeys.SHORT_NAME: "my_dag_1",
            DagConfKeys.MODEL: "my_model_1",
        },
        'my_dag_2': {
            DagConfKeys.ESSENCE: "Calls the other classification model.",
            DagConfKeys.INPUT_DAG: "my_dag_1",
            DagConfKeys.SHORT_NAME: "my_dag_2",
            DagConfKeys.MODEL: "my_model_2",
            DagConfKeys.WT: "my_wide_table",
        },
    }
}

# Create a DS Dag Manager
ds_dag_manager = DSDagManager(config=my_config)

# Create the orchestrator
ds_dag_manager.create_orchestrator(run_type='daily')

# Create your dags, and add tasks to them
dag = ds_dag_manager.create_dag('my_dag_id_1')
dag.new_task_create_table(sql='select msid, ts from my_table')
dag.new_tasks_run_model()
dag.new_tasks_store_results(append_to_history=True, update_wt=False)

dag = ds_dag_manager.create_dag('my_dag_id_2')
dag.new_task_create_table(sql='select msid, ts from my_table_2')
dag.new_tasks_run_model()
dag.new_tasks_store_results(append_to_history=True, update_wt=True)

# Call finish_creating_dags to finish creating the dags & documentation
ds_dag_manager.finish_creating_dags()
```

## New Tasks functions & parameters
The following parameters are shared by the new tasks functions:

* **is_dag_output_table**: Set this to True for the task creating the dag's output table.
* **description**: Overrides the default documentation for this task.
* **task_id, input_table, output_table** These are all optional, the defaults are usually fine here, but you can override.

DSDag has the following functions for creating tasks:

### new_task_create_table
Creates table from any of the following:
* sql sent as a parameter
* sql_index, referring to an sql in an SQLCreator. See [sc_sqls.py](https://github.com/wix-private/wix-data-dev/blob/master/ds/site_classification/dags/sc_sqls.py) for an example of creating sqls in an SQLCreator. Make sure to set this SQLCreator in your ds_dag_manager.
* spark script, by sending the lib_path, entrypoint, and (if necessary) additional_args.

By default, this will run in Spark. If you're sending the sql or sql_index parameter, you can set use_spark=False to run in a Trino Operator.

### new_tasks_run_model
Creates the following tasks:
* **run_model**: Triggers the model, taking the name from the DagConfKeys.MODEL_NAME key in the config for this dag
* **wait_for_model**: Waits for that model to finish
* **validate_results**: Validates that at least a certain percent of rows returned. That threshold is set by the config's DagConfKeys.VALIDATION_THRESHOLD (default is 99)

### new_tasks_store_results
Runs the output_ready task letting the orchestrator know it can continue.

Then does any of the following with the output:
* Appends to history table
* Updates wide table
* Writes events

### new_tasks_run_github_workflow
Triggers a GitHub workflow and waits for it to finish.

### new_task_join_result
Left joins (on msid) the prediction_table (minus features) into the input_table to create the output_table.

### new_task_write_dropped_rows
Finds the rows that were in the input table but are not in the output table, and adds them to the dropped rows table.

### new_task_python_operator
Runs a python function.

### new_task_sensor
Creates a sensor.

### new_task_branch
Creates a branch operator.

### new_task_drop_temps
Drops the temporary tables created by and for this dag.

## local_config.py
When you run on local airflow, it will use the local_config variable in [local_config.py](https://github.com/wix-private/wix-data-dev/blob/master/ds/ds_dag/dags/local_config.py) to override values from the main config object, and prevent you from modifying production tables. You can change the values (or delete the keys) in your local config (but please don't commit your changes to this file to master).

Currently, the config contains:
* **prefix:** Replace instances of 'prod.' with this prefix. If you don't want a prefix, delete this key or set it to an empty string.
* **sql_row_limit:** Limit the number of rows returned by rows that recognize this parameter (including any sql that uses the SQLCreator). If you don't want this limit, delete this key or set it to -1.
* **events_limit:** For dags that write events, limit the number of events written to this. If you don't want this limit, delete this key or set it to -1.

## Sample Usages

### Simple Use: Update Output Table
The [Update Output Table Dag](https://bo.wix.com/airflow-prod/dags/sc_update_output) is a very simple dag created using the DS Dag Framework.

It's built by [update_output_table.py](https://github.com/wix-private/wix-data-dev/blob/master/ds/site_classification/dags/update_output_table.py).

The advantages of building it through the DS Dag Framework instead of directly through Airflow operators are:
* Automatic documentation (open the DAG Docs section of the dag).
* Can use local_config.py so that when you test on local airflow it will write to a different catalog and schema, and will write fewer rows.
* The technical work around the sql (drop temp table, create temp table, drop real table, rename temp to real) is performed by the framework.


### Advanced Use: Site Classification
The [Site Classification project](https://bo.wix.com/airflow-prod/dags/site_classification_daily_orchestrator) uses most of DS Dag Framework's functionality, so looking at the Site Classification files is a good way to understand how to make advanced use of the framework.

These are the Site Classification files:
* [create_dags.py](https://github.com/wix-private/wix-data-dev/blob/master/ds/site_classification/dags/create_dags.py): creates the dags.
* [sc_config.py](https://github.com/wix-private/wix-data-dev/blob/master/ds/site_classification/dags/sc_config.py): the configuration.
* [sc_sqls.py](https://github.com/wix-private/wix-data-dev/blob/master/ds/site_classification/dags/sc_sqls.py): the SQL queries.
* [sc_events.py](https://github.com/wix-private/wix-data-dev/blob/master/ds/site_classification/dags/sc_events.py): the queries that create the events.
* [sc_dag_ids.py](https://github.com/wix-private/wix-data-dev/blob/master/ds/site_classification/dags/sc_dag_ids.py): the list of the Site Classification dags.
