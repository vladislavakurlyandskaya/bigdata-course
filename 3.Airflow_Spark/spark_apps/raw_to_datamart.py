import sys

from pyspark.sql import SparkSession, DataFrame
from utils.spark_utils import parse_spark_args
from functools import reduce


class LocalFileToRaw:

    def __init__(self, args: list):
        self.parsed_args = parse_spark_args(["table_name", "query"], args)
        self.table_name = self.parsed_args.table_name
        self.query = self.parsed_args.query

    def run(self):
        spark = SparkSession.builder.appName("LocalFileToRaw").getOrCreate()

        spark.read.jdbc("postgresql://airflow:airflow@postgres:5432/postgres", "customer")
        spark.read.jdbc("postgresql://airflow:airflow@postgres:5432/postgres", "payment")

        df = spark.sql(self.query)

        df.write.jdbc("postgresql://airflow:airflow@postgres:5432/postgres", table=self.table_name, mode="overwrite")


if __name__ == '__main__':
    LocalFileToRaw(sys.argv).run()
