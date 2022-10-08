import pyspark

from job_tutorial.util import spark, configuration, parse_args

def test_set_up_spark_session(test_container):
    sp = spark.spark()

    assert isinstance(sp, pyspark.sql.session.SparkSession)
    assert sp.conf.get('hive.exec.dynamic.partition.mode') == 'nonstrict'
    assert sp.conf.get('hive.exec.dynamic.partition') == 'true'


def test_file_args_parser():
    file_args = ["--file", "tests/fixtures/table1_rows.json"]

    args = parse_args.parse_args(file_args)

    assert args.file[0] == "tests/fixtures/table1_rows.json"


def test_set_up_config(test_container):
    assert configuration.config_for(['tutorialTable1', 'table']) == "tutorial_table1"
