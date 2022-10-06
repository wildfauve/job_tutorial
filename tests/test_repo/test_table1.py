from job_tutorial.util import spark, configuration
from job_tutorial.repo import repo_inject

def test_establish_table1_fixture(test_container, init_db, create_table1):
    df = spark.spark().table(configuration.config_for(['tutorialTable1', 'fully_qualified']))

    assert df.columns == ['id', 'name', 'pythons', 'season']

    sketches = [row.name for row in df.select(df.name).collect()]

    assert sketches == ['The Piranha Brothers', 'The Spanish Inquisition']

def test_read_table_1(test_container, init_db, create_table1):
    df = repo_inject.tutorial_table1_repo().read()

    assert df.columns == ['id', 'name', 'pythons', 'season']

