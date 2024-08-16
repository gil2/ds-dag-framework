"""
Merges select_fields from delta_table into wide_table on join_fields (default is msid).
date_column (default is execution_date) can be a comma delimited list.
If the date_column in delta_table is greater than the date_column in wide_table, the row is updated.
"""
import argparse

import pyspark.sql.functions as F
from wix_legacy_logger.logger import WixLogger
from wixspark import SparkSessionBuilder, SparkSqlWriter
from wixspark.parquet_table import ParquetTableLoader

logger = WixLogger(__file__, level=WixLogger.INFO)


def set_merge_on_read(spark, table):
    SparkSqlWriter(spark).sql(
        f"ALTER TABLE {table}  SET TBLPROPERTIES ('write.delete.mode'='merge-on-read')")


def merge_to_wt():
    configs = [("spark.dynamicAllocation.maxExecutors", '100'),
               ("spark.yarn.queue", "ds")]

    app_name = f"merge_to_wt"
    spark = SparkSessionBuilder.create_spark_session(app_name=app_name,
                                                     configs=configs)
    spark.sparkContext.setLogLevel("ERROR")
    delta_table = args.delta_table.split('.')

    delta_df = ParquetTableLoader(spark, delta_table[0], delta_table[1]).load(delta_table[2])

    if args.select_fields:
        select_fields = args.select_fields.split(',')
        delta_df = delta_df.select(*select_fields)

    # Special handling for language detection WT. I'm not happy about this.
    if 'language_detection' in args.wide_table:
        delta_df = delta_df.select('msid', 'language', F.to_date('revision_date').alias('site_change_date'))

    # If there's an 'execution_date' column make sure it's a date
    if 'execution_date' in delta_df.columns:
        # Cast the 'execution_date' column to DateType
        delta_df = delta_df.withColumn('execution_date', F.col('execution_date').cast('date'))

    if args.add_date_updated:
        if 'date_updated' not in delta_df.columns:
            delta_df = delta_df.withColumn("date_updated", F.current_timestamp())

    delta_df.createOrReplaceTempView('delta_df')
    if not spark.catalog.tableExists(args.wide_table):
        logger.info(f"table {args.wide_table} does not exist, creating it")
        SparkSqlWriter(spark).sql(f"CREATE TABLE {args.wide_table} AS SELECT * FROM delta_df")
        return
    set_merge_on_read(spark, args.wide_table)
    delta_dates = [f"delta_table.{col}" for col in args.date_column.split(',')]
    wt_dates = [f"wt.{col}" for col in args.date_column.split(',')]
    merge_query = f'''MERGE INTO {args.wide_table}     wt
                     USING delta_df           delta_table
                     ON wt.{args.join_column} = delta_table.{args.join_column}
                     WHEN MATCHED AND coalesce({",".join(delta_dates)}) > coalesce({",".join(wt_dates)}) THEN UPDATE SET *
                     WHEN NOT MATCHED THEN INSERT * 
                     '''
    SparkSqlWriter(spark).sql(merge_query)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--delta_table", type=str)
    parser.add_argument("--wide_table", type=str)
    parser.add_argument("--select_fields", type=str, default="")
    parser.add_argument("--join_column", type=str, default="msid")
    parser.add_argument("--date_column", type=str, default="execution_date")
    parser.add_argument("--add_date_updated", action='store_true')
    args = parser.parse_args()
    merge_to_wt()
