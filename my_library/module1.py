from delta.tables import *
from databricks.sdk.runtime import *


def CreateDeltaTableTst(dataFrame, targetTableFqn, dataLakePath):
  dataFrame.write \
    .format("delta") \
    .option("mergeSchema", "true") \
    .mode("overwrite") \
    .save(dataLakePath)
  spark.sql(f"CREATE TABLE IF NOT EXISTS {targetTableFqn} USING DELTA LOCATION \'{dataLakePath}\'")