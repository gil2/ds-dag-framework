local_config = {
    'prefix': "sandbox.gilr_",  # Replace all instances of 'prod.' with this prefix. If you don't want a prefix, delete this key or set it to an empty string.
    'sql_row_limit': 10_000,  # Limit the number of rows returned by rows that recognize this parameter (including any sql that uses the SQLCreator). If you don't want this limit, delete this key or set it to -1.
    'events_limit': 10   # For dags that write events, limit the number of events written to this. If you don't want this limit, delete this key or set it to -1.
}
