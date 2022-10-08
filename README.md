# Python Databricks Job Tutorial

## Poetry and Virtual Environments

## Setting up the Env

This tutorial is a collection of branches linked to each other, with each branch dealing with a specific tutorial. You
start on `main`. At the beginning of each tutorial checkout the branch defined at the top of the tutorial.

## Setting up A Poetry Env

Before we start with this tutorial, let's have a quick look at setting up a python project from scratch.

We create a new python project (using Poetry) as follows.

```bash
poetry new random_python_project_using_poetry
cd random_python_project_using_poetry
```

Now we'll add some dependencies. Firstly, to set up the dev packages.

```bash
poetry add pytest --group dev
poetry add pdbpp --group dev
poetry add pytest-env --group dev
poetry add databricks-cli --group dev
```

We can also add any python project from GIT.

```bash
poetry add git+https://github.com/wildfauve/databricker#main
poetry add git+https://github.com/wildfauve/jobsworth.git#main
```

To do anything with Spark and Databricks we'll add the common pyspark and delta libraries

```bash
poetry add pyspark
poetry add delta-spark
```

We're going to be using pymonad (for monads), pino (for structured logging), and a DI container.

```bash
poetry add PyMonad
poetry add pino
poetry add dependency-injector
```

The dependencies are defined in the `pyproject.toml` with the versions of each dependency in `poetry.lock`

`pyproject.toml` should now look like this.

```toml
[tool.poetry]
name = "random-python-project-using-poetry"
version = "0.1.0"
description = ""
authors = ["Col Perks <wild.fauve@gmail.com>"]
readme = "README.md"
packages = [{ include = "random_python_project_using_poetry" }]

[tool.poetry.dependencies]
python = "^3.9"
databricker = { git = "https://github.com/wildfauve/databricker", rev = "main" }
jobsworth = { git = "https://github.com/wildfauve/jobsworth.git", rev = "main" }
pyspark = "^3.3.0"
delta-spark = "^2.1.0"
PyMonad = "^2.4.0"
pino = "^0.6.0"
dependency-injector = "^4.40.0"


[tool.poetry.group.dev.dependencies]
pytest = "^7.1.3"
pdbpp = "^0.10.3"
pytest-env = "^0.6.2"
databricks-cli = "^0.17.3"

[build-system]
requires = ["poetry-core"]
build-backend = "poetry.core.masonry.api"
```

## Setting up the next tutorial

OK. We're now ready to clone this repo and start the tutorial. Remove the test project.

```bash
rm -rf random_python_project_using_poetry
```

```bash
git clone git@github.com:wildfauve/job_tutorial.git
git checkout main
poetry install
```

## Tutorial1: Setting Up DI

`git checkout tutorial1-set-up-di`

We'll use DI to manage dependencies, especially for Spark-based resources. While our local environment is essentially
equivalent to the env of a Databricks Spark cluster, there are some differences between the delta open source project
and delta available on the cluster. Another example is that dbutils is not available locally. DI is a good mechanism for
dealing with this problem.

Our project DI container looks like this..

```python
from dependency_injector import containers, providers

from job_tutorial.util import session
from job_tutorial.repo import db


class Container(containers.DeclarativeContainer):
    config = providers.Configuration()

    session = providers.Callable(session.di_session,
                                 session.create_session,
                                 session.spark_session_config)

    database = providers.Factory(db.Db,
                                 session,
                                 config)
```

We'll create a config file which provides a config dictionary, and initialise a single DB class with the config and the
spark session.

And we'll create an initialiser for the container.

```python
mods = ['job_tutorial.util.spark',
        'job_tutorial.util.configuration']


def build_container():
    if not env.Env().env == 'test':
        init_container()


def init_container():
    di = di_container.Container()
    di.config.from_dict(config.config)
    di.wire(modules=mods)
    return di
```

The `mods` define the modules into which the DI container will be injected (using the `@inject` decorator). Lets look at
the `configuration.py`

```python
from typing import List, Dict, AnyStr, Union
from dependency_injector.wiring import Provide, inject

from job_tutorial.di_container import Container
from job_tutorial.util import fn


def config_for(elements: List) -> Union[Dict, AnyStr]:
    return fn.deep_get(di_config(), elements)


@inject
def di_config(cfg=Provide[Container.config]) -> Dict:
    return cfg
```

Notice the use of type annotations.

Now we want to override the DI container and configuration in our tests.

We set up a testing config `config_for_testing.py` (notice the db path has changed from a DBFS path to a local path), we
remove these in our .gitignore.

```python
import pytest

from dependency_injector import containers, providers

from job_tutorial.initialiser import container
from job_tutorial.util import session
from job_tutorial.repo import db

from tests.shared import spark_test_session, config_for_test


class OverridingContainer(containers.DeclarativeContainer):
    config = providers.Configuration()

    session = providers.Callable(session.di_session,
                                 spark_test_session.spark_delta_session,
                                 spark_test_session.spark_session_config)

    database = providers.Factory(db.Db,
                                 session,
                                 config)


@pytest.fixture
def test_container():
    return init_test_container()


def init_test_container():
    di = container.init_container()
    over = OverridingContainer()
    over.config.from_dict(config_for_test.config)
    di.override(over)
    return over

```

Here we initialise the main container, then override it for testing. We're providing a test dependency for the spark
session and the config. Let's have a look at the spark session in more detail.

Setting up our test spark session is different from the spark context provided on the cluster. We're using the
utility `session.di_session` to establish the session. Its very simple...

```python
def di_session(create_fn: Callable = create_session, config_adder_fn: Callable = fn.identity) -> SparkSession:
    sp = create_fn()
    config_adder_fn(sp)
    return sp
```

We pass in 2 callables (references to functions). One to build the session, the other to apply any session config. For
testing the session builder function is `spark_test_session.spark_delta_session`

```python
import pyspark


def delta_builder():
    return (pyspark.sql.SparkSession.builder.appName("test_delta_session")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
```

And the configuration callable is...

```python
def spark_session_config(spark):
    spark.conf.set('hive.exec.dynamic.partition', "true")
    spark.conf.set('hive.exec.dynamic.partition.mode', "nonstrict")
```

Finally, once the container is overridden, we can get access to the session via a module which returns the session from
the container. This is `util.spark.py`

```python
from dependency_injector.wiring import Provide, inject
from job_tutorial.di_container import Container


@inject
def spark(session=Provide[Container.session]):
    return session
```

Let's try this out in the console. Run `poetry run python`

```python
from tests.shared import *
from job_tutorial.util import spark, configuration

init_test_container()
sp = spark.spark()
config = configuration.di_config()
```

More importantly, let's do the same in a test. We use the test container pytest fixture in the tests. But otherwise the
test looks roughly the same.

```python
def test_set_up_spark_session(test_container):
    sp = spark.spark()

    assert isinstance(sp, pyspark.sql.session.SparkSession)
```

We can use the debugger (either from the common library or using an external lib as we do here) in most places by
calling the `breakpoint()` function.

## Tutorial 2: Hive Table Fixtures and Reading Delta Tables into Dataframes

`git checkout tutorial2-hive-table`

Our target "pipeline" in the job will be:

+ Start with an existing Hive Table (which might be an operational data product)
+ Perform a transform
+ Append to a new Hive Table

The first thing we want to do is set up some test fixtures defining the input table.

We'll create a simple multi-object JSON file here `tests/fixtures/table1_rows.json`. Then define a pytest fixture to
read that into a dataframe and save it as a delta table.

```python
@pytest.fixture
def create_table1():
    (table1_test_df().write
     .format(configuration.config_for(["table_format"]))
     .mode("append")
     .saveAsTable(configuration.config_for(["tutorialTable1", "fully_qualified"])))
```

This creates a table, HIVE and Delta metadata at `spark-warehouse/tutorialdomain.db`. This looks exactly the same as it
might on the cluster.

One way to read this table into a dataframe is to use the spark session directly. Checkout the
test `tests/test_repo/test_table1.py`.

```python
def test_establish_table1_fixture(test_container, init_db, create_table1):
    df = spark.spark().table(configuration.config_for(['tutorialTable1', 'fully_qualified']))

    assert df.columns == ['id', 'name', 'pythons', 'season']

    sketches = [row.name for row in df.select(df.name).collect()]

    assert sketches == ['The Piranha Brothers', 'The Spanish Inquisition']
```

We want to put all this common repository logic in one place, instead of tests and other layers interacting with spark
DFs directly, we can mediate that through a repository model.

First we'll create a simple repo class for Table1.

```python
class TutorialTable1:
    config_root = "tutorialTable1"

    def __init__(self, db):
        self.db = db
```

That dependency is the `db.py` we created previously and is already in the DI container. So, lets inject the DB
dependency so that the repo has access to the Spark session. Lets add the repo to the container.

```python
class Container(containers.DeclarativeContainer):
    tutorial_table1 = providers.Factory(tutorial_table1.TutorialTable1,
                                        database)
```

And we'll create a module that allows us to get the repo dependencies.

```python
from dependency_injector.wiring import Provide, inject
from job_tutorial.di_container import Container


@inject
def tutorial_table1_repo(repo=Provide[Container.tutorial_table1]):
    return repo
```

And we'll need to wire it up to the DI container. In `initialiser.container.py`

```python
mods = ['job_tutorial.util.spark',
        'job_tutorial.util.configuration',
        'job_tutorial.repo.repo_inject']
```

Now we can call the repo to read the table into a dataframe. Like so.

```python
from job_tutorial.repo import repo_inject

df = repo_inject.tutorial_table1_repo().read()
```

# Next Tutorial

In the next tutorial, we'll look at transforming the operational data product and writing it as a delta table.

```shell
git checkout tutorial3-transform
```

## Tutorial 3: Transformation and Delta Writing

`git checkout tutorial3-transform`

In this tutorial we add a dataframe transformer which executes a spark transformation on the original dataframe. Then
we'll save it to a delta table.

First, lets add a test. We're going to perform some, roughly random, transformations on our input dataset. We'll explode
a column create a new column and drop others. First the test.

```python
from job_tutorial.model import transformer


def test_transform(test_container, init_db, create_table1):
    df = repo_inject.tutorial_table1_repo().read()

    new_df = transformer.transform(df)

    rows = new_df.select(F.col("sketch.name")).distinct().collect()

    assert [row.name for row in rows] == ['The Spanish Inquisition', 'The Piranha Brothers']
```

To implement this, we'll create a new layer. The `model` layer will contain our domain logic. Our transformer looks like
this.

```python
from pyspark.sql import dataframe
from pyspark.sql import functions as F


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
```

Then we'll save the new dataframe to its own Hive table. We'll add a new repository to the repository layer, calling it,
imaginatively, `tutorial_table2`. For this simple example, the functions in `table2` look exactly like those
from `table1`.

The repos are dependency injected, so, we'll add `table2` to our DI containers in test and production.

```python
tutorial_table2 = providers.Factory(tutorial_table2.TutorialTable2, database)
```

And finally to the `repo-inject` module.

```python
@inject
def tutorial_table2_repo(repo=Provide[Container.tutorial_table2]):
    return repo
```

Now lets test that the new dataframe can be written to Hive.  We'll create a new test module at `tests/test_repo/test_table2.py`

```python
def test_write_df_to_table(test_container, init_db, create_table1):
    df = transformer.transform(repo_inject.tutorial_table1_repo().read())

    repo_inject.tutorial_table2_repo().append(df)

    table2 = repo_inject.tutorial_table2_repo().read()

    breakpoint()

    rows = table2.select(F.col("sketch.name")).distinct().collect()

    assert [row.name for row in rows] == ['The Spanish Inquisition', 'The Piranha Brothers']
```

If we breakpoint into that test, we'll see that we now have 2 hive tables at the test DB location `spark-warehouse/tutorialdomain.db`.  These will be dropped after the test is complete.

Another thing to notice is the amount of coordination the test has to perform; transforming the dataframe, writing it, reading itr back, etc.  We'll be wiring the pipeline up in the next tutorial, but for the moment to DRY out this test by adding a fixture which generates the transformed dataframe.  First we'll add a pytest fixture.

```python
@pytest.fixture
def table2_dataframe():
    df = transformer.transform(repo_inject.tutorial_table1_repo().read())
    return df
```

Then we give the fixture to the test constructor.

```python
def test_write_df_to_table_dry_out(test_container, init_db, create_table1, table2_dataframe):
    repo_inject.tutorial_table2_repo().append(table2_dataframe)
```

In the next tutorial, we'll wire up our pipeline of dataframes, hive writers and transformations.  To get there, `git checkout tutorial4-command-pipeline`

## Tutorial4: Command Pipeline

`git checkout tutorial4-command-pipeline`

In this tutorial, we will wire up our various building blocks to form a complete spark job.  We'll create a simple command layer which will run the pipeline, and add a job entry point to be invoked by the Spark engine.  Let's review what our pipeline needs to do.

1. Read the data, in JSON format, from object store (which would be some DBFS mount on the Databricks cluster, but in our tests its just a test fixture).
2. Write the JSON data (well, the dataframe) to Table1.
3. Read from Table1 and transform the table1 dataframe.
4. Write the transformed dataframe to table2.

We've been working inside out; starting with the models and repos.  Now we'll move up a layer to wire up the pipeline.  This is our command layer, which will contain the orchestration.  Let's create a test first.

```python
from job_tutorial.command import minimal_pipeline


def test_pipeline_returns_success(test_container, init_db):
    result = minimal_pipeline.run("tests/fixtures/table1_rows.json")

    assert result.is_right()
    assert result.value.object_location == "tests/fixtures/table1_rows.json"
```

The pipeline command takes a file location and returns a result wrapped in a Result monad.  We'll be using result monads to wrap our pipeline commands to take advantage of function composition and error handling.  The Result monad is simply a container wrapping a value, with the result either being a "left" or "right", an error or success.

Now let's describe our pipeline as a collection of composable functions, which will do nothing apart from returning a success.  These are our sub-commands.  First we'll create a value dataclass we can pass to each function.  This provides a common interface for each sub-command.  The value looks like this.

```python
from pyspark.sql import dataframe
from dataclasses import dataclass

@dataclass
class DataClassAbstract:
    def replace(self, key, value):
        setattr(self, key, value)
        return self

@dataclass
class PipelineValue(DataClassAbstract):
    object_location: str
    input_dataframe: dataframe.DataFrame = None
```

Our minimal pipeline, which does nothing, represents the shape of the pipeline.

```python
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
```

Our this pipeline should return a Right wrapping the value.  Notice we're using the a compose operator (`>>`).  This is not part of the common library, rather it's provided by the `PyMonad` library and some extensions provided in `job_tutorial/util/monad.py`.  This is the classic left to right composition.  Each function in the compose steps just needs to return a `monad.Right` or a `monad.Left`.  The compose unwraps the value and passes it to the next function.   

Running this test should pass. 

```shell
poetry run python -m pytest tests/test_command/test_pipeline.py::test_minimal_pipeline_returns_success
```

Now, to the actual pipeline.  We want to get this test passing.

```python
def test_pipeline_returns_success(test_container, init_db):
    result = pipeline.run("tests/fixtures/table1_rows.json")

    assert result.is_right()

    table2 = repo_inject.tutorial_table2_repo().read()

    rows = table2.select(F.col("sketch.name")).distinct().collect()

    assert [row.name for row in rows] == ['The Spanish Inquisition', 'The Piranha Brothers']
```

For the purposes of the exercise, the pipeline step logic (building dataframe, writing Hive tables, etc) will be included in each subcommand.  In a more substantial job we would most likely move the logic into another layer, say the model layer with the transformer.

An example of a sub-command looks like this.

```python
def read_data(val: value.PipelineValue) -> monad.EitherMonad[value.PipelineValue]:
    df = spark.spark().read.json("tests/fixtures/table1_rows.json", multiLine=True, prefersDecimal=True)

    return monad.Right(val.replace('input_dataframe', df))
```

If we run our test we should see a pass.

```shell
poetry run python -m pytest tests/test_command/test_pipeline.py::test_minimal_pipeline_returns_success
```


Now let's see how we might handle exceptions.  First add the pytest-mock library to the project.

```shell
poetry add pytest-mock --group dev
```

We'll cause the transformer to raise an exception by mocking the transformer function.  Our test looks like this.  We would like the command to return a Left monad to us, with its value being some exception class.

```python
def test_pipeline_unexpected_exception(test_container, init_db, mocker):
    def raise_exception(_df):
        raise Exception('Boom!')

    mocker.patch('job_tutorial.model.transformer.transform', raise_exception)

    result = pipeline.run("tests/fixtures/table1_rows.json")

    assert result.is_left()
    assert result.error().message == "Boom!"
```

We use the pytest-mock mocker to patch the transform function to raise an error.  Let's run that and see what happens.

```shell
 poetry run python -m pytest tests/test_command/test_pipeline.py::test_pipeline_unexpected_exception
```

As expected, the test blows up.  It never reaches the assert statement.  What we want to happen is for the exception to be caught and turned into a Result monad (a Left in this case) containing the error (an error class).  We could use the common python `try/except` in those places where we think an error might be raised.  However, we're going to use wrapped Result monads everywhere, rather than try/except.  So, in Result monad terms what we actually want is a sort of `Try` monad.  Something that can wrap a function which might raise an exception, catch it, and wrap it in a Left Result.  For this we'll use a Try decorator.  We have one already in the `util.monad` module.  We'll ignore the detail for the moment.  Its job is to wrap a function which might raise an exception (for example, an external library call like pyspark).  

In our test, we've mocked the `transform` function, so to insert the try monad we'll need a function that calls the transform.  Its here we can add the try decorator.

```python
from job_tutorial.util import monad, error

@monad.monadic_try(error_cls=error.JobError)
def apply(df: dataframe.DataFrame) -> dataframe.DataFrame:
    return transform(df)
```

We'll create a new pipeline module to use the try-based transform.  In that sub-command, we'll want to test the result from the transformation.  It should be a result (Left in this case).  We'll return a Left wrapping the exception class.  The new transform command now looks like this.

```python
def transform(val: value.PipelineValue) -> monad.EitherMonad[value.PipelineValue]:
    result = transformer.apply(val.input_dataframe)

    if result.is_left():
        return result

    return monad.Right(val.replace('output_dataframe', result.value))
```

Returning a Left will stop the composition pipeline at the transform step.  It won't attempt to write the dataframe to table2.

And the new test now passes.

```shell
poetry run python -m pytest tests/test_command/test_pipeline.py::test_pipeline_try_monad
```

## Next Tutorial

In the final tutorial we complete the job setup and deploy it to a Databricks cluster.

`git checkout -b tutorial5-deploy-the-job`








