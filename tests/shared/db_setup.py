import pytest

from job_tutorial.util import spark, configuration

@pytest.fixture
def init_db():
    spark.spark().sql(f"create database IF NOT EXISTS {configuration.config_for(['database_name'])}")
    yield
    spark.spark().sql(f"drop database IF EXISTS {configuration.config_for(['database_name'])} CASCADE")
