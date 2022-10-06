import pyspark

from job_tutorial.util import spark, configuration

def test_set_up_spark_session(test_container):
    sp = spark.spark()

    assert isinstance(sp, pyspark.sql.session.SparkSession)
    assert sp.conf.get('hive.exec.dynamic.partition.mode') == 'nonstrict'
    assert sp.conf.get('hive.exec.dynamic.partition') == 'true'


def test_set_up_config(test_container):
    assert configuration.config_for(['tutorialTable1', 'table']) == "tutorialTable1"