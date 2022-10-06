from typing import Callable
from pyspark.sql import SparkSession

from job_tutorial.util import fn

def create_session():
    return SparkSession.builder.appName("TutorialJob").enableHiveSupport().getOrCreate()


def di_session(create_fn: Callable = create_session, config_adder_fn: Callable = fn.identity) -> SparkSession:
    sp = create_fn()
    config_adder_fn(sp)
    return sp


def spark_session_config(spark):
    spark.conf.set('spark.sql.jsonGenerator.ignoreNullFields', "false")
