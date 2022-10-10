from job_tutorial.model import value, transformer
from job_tutorial.repo import repo_inject
from job_tutorial.util import monad, spark


def run(object_location: str) -> monad.EitherMonad[value.PipelineValue]:
    result = (monad.Right(value.PipelineValue(object_location=object_location))
              >> read_data
              >> write_table1
              >> transform
              >> write_table2)
    return result


def read_data(val: value.PipelineValue) -> monad.EitherMonad[value.PipelineValue]:
    df = spark.spark().read.json(val.object_location, multiLine=True, prefersDecimal=True)

    return monad.Right(val.replace('input_dataframe', df))


def write_table1(val: value.PipelineValue) -> monad.EitherMonad[value.PipelineValue]:
    repo_inject.tutorial_table1_repo().append(val.input_dataframe)

    return monad.Right(val)


def transform(val: value.PipelineValue) -> monad.EitherMonad[value.PipelineValue]:
    result = transformer.apply(val.input_dataframe)

    if result.is_left():
        return result

    return monad.Right(val.replace('output_dataframe', result.value))


def write_table2(val: value.PipelineValue) -> monad.EitherMonad[value.PipelineValue]:
    repo_inject.tutorial_table2_repo().append(val.output_dataframe)

    return monad.Right(val)
