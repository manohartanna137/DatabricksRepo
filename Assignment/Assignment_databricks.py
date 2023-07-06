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


mounting_layer_json('wasbs://dataarchitecture@manstoragedemo.blob.core.windows.net/','/mnt/mountpointstorage','fs.azure.account.key.manstoragedemo.blob.core.windows.net','+tYtgZOyeg0s687h09bkDe2Vaoqt0ZUb0ZRAb8Ki8Sedxf5bSuae/JOSObRiGAMB1Cwmh7iY3jA0+AStm3dWoA==')

# COMMAND ----------

def read_csv_bronze(fileformat,path,header=True):
    df=spark.read.format(fileformat).option("header",header).load(path)
    return df

# COMMAND ----------

df_sample_csv=read_csv_bronze("csv","dbfs:/mnt/mountpointstorage/Bronze/sample_csv_output.csv")
display(df_sample_csv)

# COMMAND ----------

def read_json_bronze(fileformat,multiline,path):
    df=spark.read.format(fileformat).option("multiline",multiline).load(path)
    return df

# COMMAND ----------

df_json=read_json_bronze("json","true","dbfs:/mnt/mountpointstorage/Bronze/json_output.json")
display(df_json)

# COMMAND ----------


df_json_explode = df_json.withColumn("name",explode_outer(col("projects.name")))\
             .withColumn("status",explode_outer(col("projects.status"))).drop("projects")
display(df_json_explode)

# COMMAND ----------

 df_json_explode.filter(df_json_explode.department.isNull()).show()
 df_json_explode.filter(df_json_explode.id.isNull()).show()
 df_json_explode.filter(df_json_explode.salary.isNull()).show()
 df_json_explode.filter(df_json_explode.name.isNull()).show()
 df_json_explode.filter(df_json_explode.status.isNull()).show()

# COMMAND ----------

def withcolumnrename(df,colname,newcolname):
    df_rename=df.withColumnRenamed(colname,newcolname)
    return df_rename

# COMMAND ----------

df_json_column_rename=withcolumnrename(df_json_explode,"name","projectname")
display(df_json_column_rename)

# COMMAND ----------

