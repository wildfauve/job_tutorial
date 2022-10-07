from job_tutorial.model import value
from job_tutorial.util import monad


def run(object_location: str) -> monad.EitherMonad[value.PipelineValue]:
    result = (monad.Right(value.PipelineValue(object_location=object_location))
              >> read_data
              >> write_table1
              >> transform
              >> write_table2)
    return result


def read_data(val: value.PipelineValue) -> monad.EitherMonad[value.PipelineValue]:
    return monad.Right(val)


def write_table1(val: value.PipelineValue) -> monad.EitherMonad[value.PipelineValue]:
    return monad.Right(val)


def transform(val: value.PipelineValue) -> monad.EitherMonad[value.PipelineValue]:
    return monad.Right(val)


def write_table2(val: value.PipelineValue) -> monad.EitherMonad[value.PipelineValue]:
    return monad.Right(val)
