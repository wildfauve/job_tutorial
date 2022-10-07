import pytest

from dependency_injector import containers, providers

from job_tutorial.initialiser import container
from job_tutorial.util import session
from job_tutorial.repo import db, tutorial_table1, tutorial_table2

from tests.shared import spark_test_session, config_for_test


class OverridingContainer(containers.DeclarativeContainer):
    config = providers.Configuration()

    session = providers.Callable(session.di_session,
                                 spark_test_session.spark_delta_session,
                                 spark_test_session.spark_session_config)

    database = providers.Factory(db.Db,
                                 session,
                                 config)

    tutorial_table1 = providers.Factory(tutorial_table1.TutorialTable1,
                                        database)

    tutorial_table2 = providers.Factory(tutorial_table2.TutorialTable2,
                                        database)


@pytest.fixture
def test_container():
    return init_test_container()


def init_test_container():
    di = container.init_container()
    over = OverridingContainer()
    over.config.from_dict(config_for_test.config)
    di.override(over)
    return over
