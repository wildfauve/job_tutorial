# Python Databricks Job Tutorial

This tutorial is a collection of branches linked to each other, with each branch dealing with a specific tutorial.  You start on `main`.  At the beginning of each tutorial checkout the branch defined at the top of the tutorial. 


## Tutorial 0: Setting up the Env

Working from any working folder.

```bash
poetry new job_tutorial_build_example
cd job_tutorial_build_example
git init
git add .
git commit -m "first commit"
```

Set up the test and deploy (temp) environments

```bash
poetry add pytest --group dev
poetry add pdbpp --group dev
poetry add pytest-env --group dev
poetry add dataricks-cli --group dev
poetry add git+https://github.com/wildfauve/databricker#main
```

The common spark job libraries

```bash
poetry add pyspark
poetry add delta-spark
poetry add git+https://github.com/wildfauve/jobsworth.git#main
```

Add utility libs

```bash
poetry add PyMonad
poetry add pino
poetry add dependency-injector
```