"""
Creates [output_table] from [sql] using spark
Overwrites output_table if it already exists
"""

import argparse

from wix_legacy_logger.logger import WixLogger

logger = WixLogger(__file__, level=WixLogger.INFO)


def create_table_spark() -> None:
    from wixspark import SparkSessionBuilder, SparkSqlWriter
    from wixspark.parquet_table import ParquetTableStorer

    logger.info(f"args: {args}")
    configs = [("spark.dynamicAllocation.maxExecutors", '100'),
               ("spark.yarn.queue", "ds")
               ]

    app_name = f"calc_sc_create_table"
    spark = SparkSessionBuilder.create_spark_session(app_name=app_name,
                                                     configs=configs)
    spark.sparkContext.setLogLevel("ERROR")

    data = SparkSqlWriter(spark).sql(args.sql)

    output_catalog, output_schema, output_table_name = args.output_table.split('.')

    ParquetTableStorer(spark, output_catalog, output_schema).store(data=data,
                                                                   table=output_table_name,
                                                                   mode='overwrite')


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output_table", type=str)
    parser.add_argument("--sql", type=str)

    args = parser.parse_args()
    create_table_spark()
