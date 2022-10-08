from job_tutorial.command import pipeline_with_try
from jobsworth import spark_job

from job_tutorial.initialiser import container
from job_tutorial.util import parse_args

@spark_job.job()
def execute(args=None):
    parsed_args = parse_args.parse_args(args)

    result = pipeline_with_try.run(object_location=parsed_args.file[0])

    return result
