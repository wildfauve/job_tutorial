import pyspark
from delta import *


def spark_delta_session():
    return configure_spark_with_delta_pip(delta_builder()).getOrCreate()


def delta_builder():
    return (pyspark.sql.SparkSession.builder.appName("test_delta_session")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))


def spark_session_config(spark):
    spark.conf.set('hive.exec.dynamic.partition', "true")
    spark.conf.set('hive.exec.dynamic.partition.mode', "nonstrict")
