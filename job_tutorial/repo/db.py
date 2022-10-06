from job_tutorial.util import fn


class Db:
    table_config_term = "table"
    db_table_config_term = "fully_qualified"

    def __init__(self, session, config):
        self.session = session
        self.config = config
        self.create_db_if_not_exists()

    def create_db_if_not_exists(self):
        self.session.sql(f"create database IF NOT EXISTS {self.database_name()}")

    def table_exists(self, table_name):
        return table_name in self.list_tables()

    def list_tables(self):
        return [table.name for table in self.session.catalog.listTables(self.database_name())]

    def database_name(self):
        return fn.deep_get(self.config, ['database_name'])

    def table_name(self, root):
        return fn.deep_get(self.config, [root, self.table_config_term])

    def db_table_name(self, root):
        return fn.deep_get(self.config, [root, self.db_table_config_term])

    def table_format(self):
        return fn.deep_get(self.config, ['table_format'])

    def db_path(self):
        return fn.deep_get(self.config, ['db_path'])
