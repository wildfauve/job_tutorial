from pyspark.sql import dataframe
from pyspark.sql import functions as F
from job_tutorial.util import monad, error


@monad.monadic_try(error_cls=error.JobError)
def apply(df: dataframe.DataFrame) -> dataframe.DataFrame:
    return transform(df)

def transform(df: dataframe.DataFrame) -> dataframe.DataFrame:
    python_df = (df.withColumn('python', F.explode(df.pythons))
                 .withColumn('sketch', sketch_struct())
                 .drop(df.pythons)
                 .drop(df.name)
                 .drop(df.id)
                 .drop(df.season))

    return python_df


def sketch_struct():
    return (F.struct(F.col('name'),
                     F.col('season')))
