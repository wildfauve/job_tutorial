# Python Databricks Job Tutorial

This tutorial is for those who have only a limited experience with Python (or perhaps some experience with another dynamic language), and want to learn about writing and deploying jobs targeted for a Spark (pyspark) platform, and more specifically a Databricks Spark platform.

In these sets of tutorials we'll build a simple Spark job which reads and writes data to Hive tables and performs transformations using PySpark.  We'll show some ways to separate concerns in a job to improve maintainability and show how a Databricks Spark job can be tested locally with confidence that it'll work on the cluster.  Finally, we show a simple way to deploy the job and run it on a Databricks cluster (this tutorial is specific to Databricks).

The tutorials are split as follows:
1. Tutorial 0.  On branch `main`.  Shows how to set up a simple python project using poetry.
2. Tutorial 1.  On branch `tutorial1-set-up-di`.  We'll set up simple dependency inject container to allow us to DRY out things like Spark sessions and configuration.  This will also help us deal with some of the differences between local tests and the cluster.
3. Tutorial 2.  On branch `tutorial2-hive-table`.  We'll be working "inside-out".  Developing our repository logic first.  In this tutorial we set up our repository layer to allow us to work with Hive tables and Spark data frames.
4. Tutorial 3.  On branch `tutorial3-transform`.  Here, we'll look at some simple transformation logic.  While we'll be using PySpark this is not really a PySpark tutorial, so we'll only scratch the PySpark surface. 
5. Tutorial 4.  On Branch `tutorial4-command-pipeline`.  Now that we have our repository and transformation logic, the next step is to wire it all up.  So, we'll get into commands.
6. Tutorial 5.  On Branch `tutorial5-deploy-the-job`.  Finally, we'll implement the job control logic, and deploy and run the thing.


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



