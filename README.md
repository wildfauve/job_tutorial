# Python Databricks Job Tutorial

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
```

We can also add any python project from GIT.

```bash
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



