import pytest
from pyspark.sql import functions as F

from job_tutorial.command import minimal_pipeline, pipeline, pipeline_with_try
from job_tutorial.model import transformer
from job_tutorial.repo import repo_inject


def test_minimal_pipeline_returns_success(test_container, init_db):
    result = minimal_pipeline.run("tests/fixtures/table1_rows.json")

    assert result.is_right()
    assert result.value.object_location == "tests/fixtures/table1_rows.json"


def test_pipeline_returns_success(test_container, init_db):
    result = pipeline.run("tests/fixtures/table1_rows.json")

    assert result.is_right()

    table2 = repo_inject.tutorial_table2_repo().read()

    rows = table2.select(F.col("sketch.name")).distinct().collect()

    assert [row.name for row in rows] == ['The Spanish Inquisition', 'The Piranha Brothers']


def test_pipeline_unexpected_exception(test_container, init_db, mocker):
    def raise_exception(_df):
        raise Exception('Boom!')

    mocker.patch('job_tutorial.model.transformer.transform', raise_exception)

    result = pipeline.run("tests/fixtures/table1_rows.json")

    assert result.is_left()
    assert result.error().message == "Boom!"


def test_pipeline_try_monad(test_container, init_db, mocker):
    def raise_exception(_df):
        raise Exception('Boom!')

    mocker.patch('job_tutorial.model.transformer.transform', raise_exception)

    result = pipeline_with_try.run("tests/fixtures/table1_rows.json")

    assert result.is_left()
    assert result.error().message == "Boom!"

