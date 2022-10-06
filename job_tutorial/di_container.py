from dependency_injector import containers, providers

from job_tutorial.util import session
from job_tutorial.repo import db, tutorial_table1


class Container(containers.DeclarativeContainer):
    config = providers.Configuration()

    session = providers.Callable(session.di_session,
                                 session.create_session,
                                 session.spark_session_config)

    database = providers.Factory(db.Db,
                                 session,
                                 config)

    tutorial_table1 = providers.Factory(tutorial_table1.TutorialTable1,
                                        database)