# Databricks notebook source
# MAGIC %sql
# MAGIC create or replace table scd(name STRING,rollno INT,marks INT,subject STRING) USING DELTA LOCATION 'dbfs:/FileStore/manoharfile/commands'

# COMMAND ----------

from delta.tables import *
DeltaTable.create(spark) \
    .tableName('scd2') \
    .addColumn('name', 'STRING') \
    .addColumn('rollno', 'INT') \
    .addColumn('marks', 'INT') \
    .addColumn('subject', 'STRING') \
    .location('dbfs:/FileStore/manoharfile/commands/scd_table') \
    .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from scd2

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,IntegerType
data = [
    ('mano', 120, 85, 'social'),
    ('vams', 121, 86, 'english'),
    ('jaggu', 122, 87, 'maths')
]

schema = StructType([
    StructField("name", StringType(), nullable=False),
    StructField("rollno", IntegerType(), nullable=False),
    StructField("marks", IntegerType(), nullable=False),
    StructField("subject", StringType(), nullable=False)
])

df = spark.createDataFrame(data, schema)

# COMMAND ----------

df.write.insertInto('scd2',overwrite=False)

# COMMAND ----------

tableinstance=DeltaTable.forName(spark,'scd2')


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from scd2

# COMMAND ----------

# MAGIC %sql
# MAGIC describe  history scd2

# COMMAND ----------

 data1=  [ ('raju', 124, 76, 'english'),
    ('raghu', 125, 67, 'maths')
]

schema1 = StructType([
    StructField("name", StringType(), nullable=False),
    StructField("rollno", IntegerType(), nullable=False),
    StructField("marks", IntegerType(), nullable=False),
    StructField("subject", StringType(), nullable=False)
])
df1=spark.createDataFrame(data1,schema1)
display(df1)

# COMMAND ----------

df1.write.format('delta').mode('append').saveAsTable('scd2')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from scd2

# COMMAND ----------

# MAGIC %md #### restore command

# COMMAND ----------

tableinstance.restoreToTimestamp('2023-07-01T13:10:00.000+0000')

# COMMAND ----------

# MAGIC %md #### querrying after using restore command

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from scd2

# COMMAND ----------

# MAGIC %md #### optimize command
# MAGIC OPTIMIZE scd2

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE scd2

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history scd2

# COMMAND ----------

# MAGIC %fs
# MAGIC ls 'dbfs:/FileStore/manoharfile/commands/scd_table'

# COMMAND ----------

# MAGIC %sql
# MAGIC vacuum scd2 dry run

# COMMAND ----------

# MAGIC %md #### vacuum command
# MAGIC - while using the below command we get an error that the value is less than 168 hours so we are setting the value to false below cell

# COMMAND ----------

# MAGIC %sql
# MAGIC vacuum scd2 retain 0 hours dry run

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.databricks.delta.retentionDurationCheck.enabled = false

# COMMAND ----------

# MAGIC %sql
# MAGIC vacuum scd2 retain 0 hours dry run

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from scd2

# COMMAND ----------

data_ev = [('mike', 128, 85, 'social', 'A'),
    ('messi', 129, 86, 'english', 'B'),
    ('jr neymar', 130, 87, 'maths', 'A')]

schema_ev = StructType([
    StructField("name", StringType(), nullable=False),
    StructField("rollno", IntegerType(), nullable=False),
    StructField("marks", IntegerType(), nullable=False),
    StructField("subject", StringType(), nullable=False),
    StructField("grade", StringType(), nullable=False)])

df_evolution = spark.createDataFrame(data_ev, schema_ev)
display(df_evolution)

# COMMAND ----------

# MAGIC %md #### schema evolution

# COMMAND ----------

df_evolution.write.option('mergeSchema',True).format('delta').mode('append').saveAsTable('scd2')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from scd2

# COMMAND ----------

