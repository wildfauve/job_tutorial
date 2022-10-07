from pyspark.sql import functions as F
from job_tutorial.util import spark, configuration
from job_tutorial.model import transformer
from job_tutorial.repo import repo_inject


def test_transform(test_container, init_db, create_table1):
    df = repo_inject.tutorial_table1_repo().read()

    new_df = transformer.transform(df)

    rows = new_df.select(F.col("sketch.name")).distinct().collect()

    assert [row.name for row in rows] == ['The Spanish Inquisition', 'The Piranha Brothers']




