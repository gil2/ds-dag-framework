import sqlparse


class EventCreator:
    def __init__(self):
        self.event_query_type = None
        self.source_table = None
        self.events_domain = None
        self.evid = None
        self.events_table = None
        self.filter_sites_with_events_after = None
        self.sql = ""
        self.limit = None
        self.execution_date = None
        self.event_query_type = None

    def _child_class_set_sql(self):
        # Child classes should implement this method
        raise NotImplementedError

    def get_sql(self, event_query_type, source_table, execution_date, limit=None):
        self.event_query_type = event_query_type
        self.source_table = source_table
        self.execution_date = execution_date

        self._child_class_set_sql()
        if self.filter_sites_with_events_after:
            self.sql = f"""
            with events_already_written as (
                select msid 
                from events.dbo.{self.events_domain}_70
                where
                    evid = {self.evid} and
                    date_created between date '{self.filter_sites_with_events_after}' and current_date + interval '1' day
                group by 1
            )
            , events_to_write as ({self.sql})
            select events_to_write.*
            from events_to_write 
            left join events_already_written on events_to_write.msid = events_already_written.msid
            where events_already_written.msid is null
            """

        # We will respect limit=0, so we're explicit about only ignoring None and -1
        if (limit is not None) and (limit != -1):
            self.sql += f"\nlimit {limit}"

        self.sql = sqlparse.format(self.sql, trim_whitespace=True)

        # Now wrap it into a set of 3 sql statements that will
        #   create the table with the records to write as events
        #   write the table name to the events_to_write table
        self.sql = f"""
        drop table if exists {self.events_table};
        create table {self.events_table} as {self.sql};
        {get_insert_events_sql(table_name=self.events_table, evid=self.evid)}
        """

        self.sql = sqlparse.format(self.sql, trim_whitespace=True)
        self.sql = self.sql.replace("        ", "")
        return self.sql


def get_insert_events_sql(table_name, evid, source_number=70, endpoint='profile-events', drop_table=True, priority=1):
    sql = f"""
    insert into sandbox.synthetic_events_logger.tables_to_write
    select
    LOCALTIMESTAMP as date_created,
    '{table_name}' as table_name,
    '{endpoint}' as endpoint,
    {source_number} as source_number,
    {evid} as evid,
    {drop_table} as drop_table,
    {priority} as priority
    """.replace("    ", "")
    # sql = sqlparse.format(sql, trim_whitespace=True, reindent=True)
    return sql
