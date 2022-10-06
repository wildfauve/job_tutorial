import pytest
from job_tutorial.util import spark, configuration


@pytest.fixture
def create_table1():
    (table1_test_df().write
     .format(configuration.config_for(["table_format"]))
     .mode("append")
     .saveAsTable(configuration.config_for(["tutorialTable1", "fully_qualified"])))


@pytest.fixture
def table1_df():
    return table1_test_df()


def table1_test_df():
    return spark.spark().read.json("tests/fixtures/table1_rows.json", multiLine=True, prefersDecimal=True)
