"""
Left joins (on msid) the prediction_table (minus features) into the input_table to create the output_table.
Uses Spark.
"""
# Standard Library Imports
import argparse

# Third Party Imports
import pyspark.sql.functions as F
from wix_legacy_logger.logger import WixLogger
from wixspark import SparkSessionBuilder
# wix imports
from wixspark.parquet_table import ParquetTableLoader, ParquetTableStorer

# project imports

logger = WixLogger(__file__, level=WixLogger.INFO)


def do_join(spark):
    kernel = args.input_table.split('.')
    prediction = args.prediction_table.split('.')
    kernel_df = ParquetTableLoader(spark, kernel[0], kernel[1]).load(kernel[2])
    prediction_df = ParquetTableLoader(spark, prediction[0], prediction[1]).load(prediction[2]).drop(
        *args.features.split(','))
    df = kernel_df.join(F.broadcast(prediction_df), on='msid', how='left')

    output = args.output_table.split('.')
    ParquetTableStorer(spark, output[0], output[1]).store(data=df, table=output[2], mode='overwrite')


def main():
    configs = [("spark.dynamicAllocation.maxExecutors", '50'),
               ("spark.dynamicAllocation.initialExecutors", "10"),
               ("spark.executor.memory", "12g"),
               ("spark.executor.cores", "3"),
               ("spark.driver.memory", "20g"),
               ('spark.memory.fraction', '0.8'),
               ("spark.driver.maxResultSize", "16g"),
               ("spark.yarn.queue", "ds")]

    app_name = f"join prediction to kernel"
    spark = SparkSessionBuilder.create_spark_session(app_name=app_name,
                                                     configs=configs)
    spark.sparkContext.setLogLevel("ERROR")
    do_join(spark)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--prediction_table", type=str)
    parser.add_argument("--input_table", type=str)
    parser.add_argument("--output_table", type=str)
    parser.add_argument("--features", type=str)
    args = parser.parse_args()
    print(main())
