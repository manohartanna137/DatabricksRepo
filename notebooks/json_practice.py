# Databricks notebook source
from pyspark.sql.types import StructField,StructType,StringType,LongType,ArrayType
from pyspark.sql.functions import when,col,explode_outer 

# COMMAND ----------

def mounting_layer_json(source,mountpoint,key,value):
    dbutils.fs.mount(
    source=source,
    mount_point= mountpoint,
    extra_configs={key:value})

    

# COMMAND ----------

mounting_layer_json('wasbs://assignmentoutput@manstoragedemo.blob.core.windows.net/','/mnt/mountstorage','fs.azure.account.key.manstoragedemo.blob.core.windows.net','+tYtgZOyeg0s687h09bkDe2Vaoqt0ZUb0ZRAb8Ki8Sedxf5bSuae/JOSObRiGAMB1Cwmh7iY3jA0+AStm3dWoA==')

# COMMAND ----------

def read_data_from_mountpoints(fileformat, multiline, path, schema=None):
    df = spark.read.format(fileformat).option("multiline", multiline)
    if schema:
        df = df.schema(schema)
    df = df.load(path)
    return df


# COMMAND ----------

df_json=read_data_from_mountpoint("json","true",'/mnt/mountstorage/')
display(df_json)

# COMMAND ----------

projects_schema=StructType([StructField("name",StringType()),\
    StructField("status",StringType())])

element_schema = StructType(projects_schema)

root_schema = StructType([
    StructField("department", StringType()),
    StructField("id", LongType()),
    StructField("projects", ArrayType(projects_schema)),
    StructField("salary", LongType())
])

# COMMAND ----------

df_json=read_data_from_mountpoint("json","true",'/mnt/mountstorage/',schema=root_schema)
display(df_json)

# COMMAND ----------


df_json_explode = df_json.withColumn("name",explode_outer(col("projects.name")))\
             .withColumn("status",explode_outer(col("projects.status")))
display(df_json_explode)

# COMMAND ----------

df_json_explode.write.format('parquet').mode("overwrite").save('/mnt/mountstorage/Bronze/')

# COMMAND ----------

# MAGIC %md #### silver

# COMMAND ----------



# COMMAND ----------

df_json_silver = spark.read.parquet("dbfs:/mnt/mountstorage/Bronze/part-00000-tid-1446162791822775570-d5ce4818-61b3-40fb-8853-f6ae812b4529-297-1-c000.snappy.parquet")
display(df_json_silver)

# COMMAND ----------

distinct_df = df_json_silver.distinct()
display(distinct_df)

# COMMAND ----------

df_lower = distinct_df.toDF(*[name.lower() for name in distinct_df.columns])
display(df_lower)

# COMMAND ----------

df_null_values_impute=df_lower.fillna(value=0)
display(df_null_values_impute)

# COMMAND ----------

df_null_values_impute.write.format("parquet").mode("append").save('/mnt/mountstorage/Silver/')

# COMMAND ----------

