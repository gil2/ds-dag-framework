"""
Fails if less than validation_threshold (default=99) percent of the rows in kernel_table are in the output_table.
If it succeeds, writes any dropped rows to dropped_rows_table, with priority=1 and drop_step=f'model_{process_short_name}'.
"""
import argparse

import pyspark.sql.functions as F
from wix_legacy_logger.logger import WixLogger
from wixspark import SparkSessionBuilder
from wixspark.parquet_table import ParquetTableLoader, ParquetTableStorer

logger = WixLogger(__file__, level=WixLogger.INFO)


def validation_step(spark, output_table, kernel_table, dropped_rows_table, process_short_name, validation_threshold):
    output_table = output_table.split('.')
    sites_batch_predict = ParquetTableLoader(spark, output_table[0], output_table[1]).load(
        output_table[2])
    msids_batch_predict = sites_batch_predict.select("msid")
    count_batch_predict = msids_batch_predict.count()
    kernel_table = kernel_table.split('.')
    kernel_df = ParquetTableLoader(spark, kernel_table[0], kernel_table[1]).load(kernel_table[2]).select('msid')
    kernel_count = kernel_df.count()
    logger.info(f'number of full results {count_batch_predict}')
    logger.info(f'number of input msids  {kernel_count}')

    if (count_batch_predict / kernel_count) * 100 < validation_threshold:
        logger.info(f"missing fraction is greater than {100 - validation_threshold}, validation threshold is {validation_threshold}%, ABORTING")
        spark.stop()
        exit(2)

    dropped = kernel_df.select("msid").subtract(msids_batch_predict.select("msid"))
    if dropped.count() > 0:
        dropped = dropped.withColumn('drop_step', F.lit(f'model_{process_short_name}'))
        dropped = dropped.withColumn('priority', F.lit(1))
        dropped_rows_catalog, dropped_rows_schema, dropped_rows_table_name = dropped_rows_table.split('.')
        ParquetTableStorer(spark, dropped_rows_catalog, dropped_rows_schema).store(data=dropped,
                                                                       table=dropped_rows_table_name,
                                                                       mode='append')


def main():
    configs = [("spark.dynamicAllocation.maxExecutors", '50'),
               ("spark.dynamicAllocation.initialExecutors", "15"),
               ("spark.yarn.queue", "ds")]

    app_name = f"validate_results"
    spark = SparkSessionBuilder.create_spark_session(app_name=app_name,
                                                     configs=configs)
    spark.sparkContext.setLogLevel("ERROR")
    validation_step(spark,
                    output_table=args.output_table,
                    kernel_table=args.kernel_table,
                    dropped_rows_table=args.dropped_rows_table,
                    process_short_name=args.process_short_name,
                    validation_threshold=args.validation_threshold)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--kernel_table", type=str)
    parser.add_argument("--output_table", type=str)
    parser.add_argument("--validation_threshold", type=int, default=99)
    parser.add_argument("--process_short_name", type=str)
    parser.add_argument("--dropped_rows_table", type=str)
    args = parser.parse_args()
    main()
