import pytest
from pyspark.sql import functions as F

from job_tutorial.repo import repo_inject
from job_tutorial.model import transformer

def test_write_df_to_table(test_container, init_db, create_table1):
    df = transformer.transform(repo_inject.tutorial_table1_repo().read())

    repo_inject.tutorial_table2_repo().append(df)

    table2 = repo_inject.tutorial_table2_repo().read()

    breakpoint()

    rows = table2.select(F.col("sketch.name")).distinct().collect()

    assert [row.name for row in rows] == ['The Spanish Inquisition', 'The Piranha Brothers']


def test_write_df_to_table_dry_out(test_container, init_db, create_table1, table2_dataframe):
    repo_inject.tutorial_table2_repo().append(table2_dataframe)

    table2 = repo_inject.tutorial_table2_repo().read()

    rows = table2.select(F.col("sketch.name")).distinct().collect()

    assert [row.name for row in rows] == ['The Spanish Inquisition', 'The Piranha Brothers']


@pytest.fixture
def table2_dataframe():
    df = transformer.transform(repo_inject.tutorial_table1_repo().read())
    return df
