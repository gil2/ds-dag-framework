# {{ title }}

### Essence

{{ essence }}
{{ essence_extra }}
{{ more_info }}

### Tasks
This dag has the following tasks:

{{ tasks }}

### Running locally
You can use different values when running on local Airflow. Just change values in your local_config.py.

These are the local overrides currently in use:

```python
{{ local_config }}
```

### Libraries & scripts used:
This dag is created by {{ created_by }} [dsdag.py]({{ ds_dag_root }}dags/dsdag.py) 
from the [ds_dag framework]({{ ds_dag_root }}).

{{ scripts }}

This documentation is created by DS Dag's [doc_creator.py]({{ ds_dag_root }}dags/doc_creator.py) using the template [doc.md]({{ ds_dag_root }}dags/doc.md).

{{ config_file }}
