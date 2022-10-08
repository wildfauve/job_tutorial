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
    output_dataframe: dataframe.DataFrame = None
