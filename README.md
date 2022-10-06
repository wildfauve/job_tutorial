# Python Databricks Job Tutorial

## Poetry and Virtual Environments

## Setting up the Env
This tutorial is a collection of branches linked to each other, with each branch dealing with a specific tutorial.  You start on `main`.  At the beginning of each tutorial checkout the branch defined at the top of the tutorial.

## Setting up A Poetry Env

Before we start with this tutorial, let's have a quick look at setting up a python project from scratch. 

We create a new python project (using Poetry) as follows.

```bash
poetry new random_python_project_using_poetry
cd random_python_project_using_poetry
```

Now we'll add some dependencies.  Firstly, to set up the dev packages.

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
packages = [{include = "random_python_project_using_poetry"}]

[tool.poetry.dependencies]
python = "^3.9"
databricker = {git = "https://github.com/wildfauve/databricker", rev = "main"}
jobsworth = {git = "https://github.com/wildfauve/jobsworth.git", rev = "main"}
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

OK.  We're now ready to clone this repo and start the tutorial.  Remove the test project.

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

And we'll need to wire it up to the DI container.  In `initialiser.container.py`

```python
mods = ['job_tutorial.util.spark',
        'job_tutorial.util.configuration',
        'job_tutorial.repo.repo_inject']
```

Now we can call the repo to read the table into a dataframe.  Like so.

```python
from job_tutorial.repo import repo_inject

df = repo_inject.tutorial_table1_repo().read()
```

