import sqlparse


class SQLCreator:
    def __init__(self):
        self.sql_index = None
        self.input_table = None
        self.op_kwargs = None
        self.sql = None

    def get_kwarg(self, key):
        # Will crash if op_kwargs is not set or key is not found
        val = self.op_kwargs.get(key)
        if val is None:
            raise ValueError(f"key {key} is not found in op_kwargs creating sql for {self.sql_index}")

        return val

    def get_sql(self, sql_index, input_table, op_kwargs=None):
        # Should be called by the child class
        self.sql_index = sql_index
        self.input_table = input_table
        self.op_kwargs = op_kwargs

        self._child_class_set_sql()

        if not self.sql:
            raise ValueError(f"No sql found for get_site_class_sql: {self.sql_index}")

        return sqlparse.format(self.sql)

    def _child_class_set_sql(self):
        # Child classes should implement this method
        raise NotImplementedError
