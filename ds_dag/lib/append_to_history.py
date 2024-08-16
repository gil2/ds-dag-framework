"""
Appends an input (delta) table to an output (history) table.
Partitions by execution_date.
If output_cols is sent, only outputs those fields (plus the following).
Also sends run_type & execution_date (as date) if sent.
Has some special handling for language_detection output table.
"""

import argparse

import pyspark.sql.functions as F
from wix_legacy_logger.logger import WixLogger
from wixspark import SparkSessionBuilder
from wixspark.parquet_table import ParquetTableLoader, ParquetTableStorer
from wixspark.spark_utils import SparkUtils

logger = WixLogger(__file__, level=WixLogger.INFO)


def append(spark):
    input_catalog, input_schema, input_table_name = args.input_table.split('.')
    output_catalog, output_schema, output_table_name = args.output_table.split('.')
    df = ParquetTableLoader(spark, input_catalog, input_schema).load(input_table_name)

    if args.output_cols:
        output_cols = args.output_cols.split(',')
        df = df.select(*output_cols)

    if args.run_type:
        df = df.withColumn('run_type', F.lit(args.run_type))

    if args.execution_date:
        df = df.withColumn('execution_date', F.lit(args.execution_date).cast('date'))
    else:
        df = df.withColumn('execution_date', F.col('execution_date').cast('date'))

    # This is not a good solution, but we'll keep it for now
    if 'language_detection' in args.output_table:
        df = df.withColumn('visual_content', F.lit(None).cast('string'))

    df = SparkUtils.nullify_df(spark, df).repartition('execution_date').sortWithinPartitions('execution_date')
    ParquetTableStorer(spark, output_catalog, output_schema).store(data=df, table=output_table_name,
                                                                   mode='append',
                                                                   partition_by='execution_date')


def main():
    logger.info(f"args: {args}")
    configs = [("spark.dynamicAllocation.maxExecutors", '80'),
               ("spark.dynamicAllocation.initialExecutors", "20"),
               ("spark.executor.memory", "12g"),
               ("spark.executor.cores", "3"),
               ("spark.yarn.queue", "ds")]

    app_name = f"append_to_history"
    spark = SparkSessionBuilder.create_spark_session(app_name=app_name,
                                                     configs=configs)
    spark.sparkContext.setLogLevel("ERROR")
    append(spark)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_table", type=str)
    parser.add_argument("--output_table", type=str)
    parser.add_argument("--execution_date", type=str, default='')
    parser.add_argument("--run_type", type=str, default='')
    parser.add_argument("--output_cols", type=str, default=None)
    args = parser.parse_args()
    main()
