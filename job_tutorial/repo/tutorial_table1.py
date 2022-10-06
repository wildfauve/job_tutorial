from pyspark.sql import dataframe
from delta.tables import *

class TutorialTable1:
    config_root = "tutorialTable1"

    def __init__(self, db):
        self.db = db

    def delta_read(self) -> Optional[dataframe.DataFrame]:
        if not self.table_exists():
            return None
        return self.delta_table().toDF()

    def read(self):
        if not self.table_exists():
            return None
        return self.db.session.table(self.db_table_name())

    def read_stream(self):
        return (self.db.session
                .readStream
                .format('delta')
                .option('ignoreChanges', True)
                .table(self.db_table_name()))

    def create(self, df, *partition_cols):
        (df.write
         .format(self.db.table_format())
         .partitionBy(partition_cols)
         .mode("append")
         .saveAsTable(self.db_table_name))

    def upsert(self, df, *partition_cols):
        if not self.table_exists():
            return self.create(df, *partition_cols)


        (self.delta_table().alias('table1')
         .merge(
            df.alias('updates'),
            'table1.identity = updates.identity')
         .whenNotMatchedInsertAll()
         .execute())

    def delta_table(self) -> DeltaTable:
        return DeltaTable.forPath(self.db.session, self.db_table_path())

    def table_exists(self) -> bool:
        return self.db.table_exists(self.table_name())

    def db_table_name(self):
        return self.db.db_table_name(self.config_root)

    def table_name(self):
        return self.db.table_name(self.config_root)

    def db_table_path(self):
        return f"{self.db.db_path()}/{self.table_name()}"

