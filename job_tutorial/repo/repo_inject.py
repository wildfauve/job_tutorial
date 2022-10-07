from dependency_injector.wiring import Provide, inject
from job_tutorial.di_container import Container


@inject
def tutorial_table1_repo(repo=Provide[Container.tutorial_table1]):
    return repo

@inject
def tutorial_table2_repo(repo=Provide[Container.tutorial_table2]):
    return repo
