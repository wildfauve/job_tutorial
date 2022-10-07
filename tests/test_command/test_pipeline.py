from job_tutorial.command import minimal_pipeline

def test_pipeline_returns_success(test_container, init_db):
    result = minimal_pipeline.run("tests/fixtures/table1_rows.json")

    assert result.is_right()