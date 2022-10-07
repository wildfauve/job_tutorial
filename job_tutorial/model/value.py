from pyspark.sql import dataframe
from dataclasses import dataclass

@dataclass
class PipelineValue:
    object_location: str
    input_dataframe: dataframe.DataFrame = None
