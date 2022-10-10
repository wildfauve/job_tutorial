from job_tutorial import job
from job_tutorial.model import value

def test_job_completes_successfully(test_container, init_db):
    file_args = ["--file", "tests/fixtures/table1_rows.json"]
    result = job.execute(args=file_args)

    assert result.is_right()
    assert isinstance(result.value, value.PipelineValue)
