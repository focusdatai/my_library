from delta.tables import *

def CreateDeltaTableTst(dataFrame, targetTableFqn, dataLakePath):
  dataFrame.write \
    .format("delta") \
    .option("mergeSchema", "true") \
    .mode("overwrite") \
    .save(dataLakePath)
  spark.sql(f"CREATE TABLE IF NOT EXISTS {targetTableFqn} USING DELTA LOCATION \'{dataLakePath}\'")


def CreateDeltaTable(dataFrame, targetTableFqn, dataLakePath):
  dataFrame.write \
    .format("delta") \
    .option("mergeSchema", "true") \
    .mode("overwrite") \
    .save(dataLakePath)
  spark.sql(f"CREATE TABLE IF NOT EXISTS {targetTableFqn} USING DELTA LOCATION \'{dataLakePath}\'")

def BasicMerge(sourceDataFrame, targetTableFqn, businessKey=None):
  businessKey = spark.table(targetTableFqn).columns[0] if businessKey is None else businessKey
  s = ConcatBusinessKey(businessKey, "s")
  t = ConcatBusinessKey(businessKey, "t")
  
  df = DeltaTable.forName(spark, targetTableFqn).alias("t").merge(sourceDataFrame.alias("s"), f"{s} = {t}") \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()
  
  def CreateOrMerge(sourceDataFrame, targetTableFqn, dataLakePath, businessKey=None):
  if (TableExists(targetTableFqn)):
    BasicMerge(sourceDataFrame, targetTableFqn, businessKey)
  else:
    CreateDeltaTable(sourceDataFrame, targetTableFqn, dataLakePath)  