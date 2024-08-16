"""
Creates output_table with features from input_table.
Overwrites output_table if it already exists.
"""
import argparse

from wix_legacy_logger.logger import WixLogger
from wixspark import SparkSessionBuilder
from wixspark.parquet_table import ParquetTableLoader, ParquetTableStorer

logger = WixLogger(__file__, level=WixLogger.INFO)


def project(spark):
    table = args.input_table.split('.')
    out_table = args.output_table.split('.')
    features = args.features.split(',')

    df = ParquetTableLoader(spark, table[0], table[1]).load(table[2]).select('msid', *features)
    ParquetTableStorer(spark, out_table[0], out_table[1]).store(table=out_table[2], data=df, mode='overwrite')


def main():
    configs = [("spark.dynamicAllocation.maxExecutors", '30'),
               ("spark.dynamicAllocation.initialExecutors", "5"),
               ("spark.executor.memory", "12g"),
               ("spark.executor.cores", "3"),
               ("spark.driver.memory", "20g"),
               ('spark.memory.fraction', '0.8'),
               ("spark.driver.maxResultSize", "16g"),
               ("spark.yarn.queue", "ds")]

    app_name = f"project_features"
    spark = SparkSessionBuilder.create_spark_session(app_name=app_name,
                                                     configs=configs)
    spark.sparkContext.setLogLevel("ERROR")
    return project(spark)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_table", type=str)
    parser.add_argument("--output_table", type=str)
    parser.add_argument("--features", type=str)
    args = parser.parse_args()
    main()
