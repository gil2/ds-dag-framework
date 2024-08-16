# {{ title }}

### Essence
    
{{ essence }}
{{ more_info }}

### How to run a backfill
To run a backfill, do a Trigger with Conf on the backfill orchestrator.

You will then see the default configuration for the internal dags.

The order in which the internal dags appear here doesn't matter.

You can override any of these parameters, and also choose which internal dags to run.

To not run a specific internal dag, just delete it from the conf. 

{{ special_backfill_instructions }}You can also use the backfill orchestrator for ad-hoc runs. Just make sure that if you don't want to write to the production tables, that you override the table names.

### The Internal Dags
The orchestrator calls the following internal dags:

{{ internal_dags }}

### Internal Dags Task Groups
For each of the internal dags listed above, the Orchestrator has a task group with the following tasks:
1. **enrich_dag_input**: Calculates the configuration with which to trigger the internal dag. Most of this comes from {{ config_link }}. The following things are different if you're running a backfill:
   1. You will be able to fill in the parameters to override defaults, and choose which internal dags to run
   2. This task will be called **check_if_run**, and will skip the rest of the tasks if this internal dag was not listed in the dags section of the conf you provide.
2. **trigger_dag**: Triggers the internal dag.
3. **wait_for_dag**: Waits for the internal dag's "output_ready" task to run.
4. **dropped_rows**: Adds any rows that were dropped by this internal dag to the dropped rows table.

### Output Ready, Store Output, and Output Stored
Each internal dag takes an input table, performs some process(es), and creates an output table.

That output table is usually an input of a subsequent internal dag. For example, the output of sc_feature_enrichment is the input of sc_language_detection.

The internal dags then perform the "store output" tasks, which always include writing the internal dag's history table, and sometimes also include updating a wide table and / or writing events. For specific details of these, see [History Tables](#history-tables), [Wide Tables](#wide-tables), and [Events](#events).

Once the output table is ready, the orchestrator can proceed to the next internal dag, which can run in parallel to this dag storing its results.

Before storing results, the internal dags run an **output_ready** task, an empty operator which the Orchestrator waits for, to know it can continue.

After storing results, the internal dags run an **output_stored** task, which is also an empty operator, used to signal that subsequent tasks (like dropping temps) can run. 


### Tasks

{{ tasks }}

### History Tables
These are the history tables:

{{ history_tables }}

### Wide Tables
This pipeline updates the following wide tables:

{{ wide_tables }}

### Events
{{ event_info }}

### Running locally
You can use different values when running on local Airflow. Just change values in your local_config.py.

These are the local overrides currently in use:

```python
{{ local_config }}
```

### Libraries & scripts used:
This dag is created by {{ created_by }} [orchestrator.py]({{ ds_dag_root }}dags/orchestrator.py) 
from the [ds_dag framework]({{ ds_dag_root }}).

{{ scripts }}

This documentation is created by DS Dag's [doc_creator.py]({{ ds_dag_root }}dags/doc_creator.py) using the template [orchestrator_doc.md]({{ ds_dag_root }}dags/orchestrator_doc.md).

{{ config_file }}

