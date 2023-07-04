# Databricks notebook source
def read_data(fileformat,path,delimeter=",",header=True):
    df = spark.read.format(fileformat).option('header',header).option('delimeter',delimeter).load(path)
    return df


# COMMAND ----------

df_bronze = read_data("csv","dbfs:/FileStore/Assignment_databricks_manohar/data_files/sample_csv.csv")
display(df_bronze)

# COMMAND ----------

df_bronze.write.format('parquet').mode("overwrite").save('dbfs:/FileStore/Assignment_databricks_manohar/Bronze')

# COMMAND ----------

def read_csv_parquet(path):
    df_csv=spark.read.parquet(path)
    return df_csv



# COMMAND ----------

# part-00000-tid-5913520315788759522-f1f1e3a2-a51d-4a6b-9369-0d7193371712-34-1-c000.snappy.parquet

# COMMAND ----------

df_csv_parquet=read_csv_parquet('dbfs:/FileStore/Assignment_databricks_manohar/Bronze/')
display(df_csv_parquet)

# COMMAND ----------



# COMMAND ----------

def bronze_layer(source,mountpoint,key,value):
    dbutils.fs.mount(
    source=source,
    mount_point= mountpoint,
    extra_configs={key:value})

# COMMAND ----------

bronze_layer('wasbs://assignmentoutput@manstoragedemo.blob.core.windows.net/', '/mnt/storagema','fs.azure.account.key.manstoragedemo.blob.core.windows.net','+tYtgZOyeg0s687h09bkDe2Vaoqt0ZUb0ZRAb8Ki8Sedxf5bSuae/JOSObRiGAMB1Cwmh7iY3jA0+AStm3dWoA==')


# COMMAND ----------

df_bronze=read_data("csv",'/mnt/storagema')
display(df_bronze)

# COMMAND ----------

df_bronze.write.format('parquet').mode("overwrite").save('/mnt/storagema/Bronze/')

# COMMAND ----------

df_bronze.write.format('parquet').mode("overwrite").save('/mnt/storagema/Silver')

# COMMAND ----------


multiline_df = spark.read.option("multiline","true").json("/mnt/storagema/json_output")
display(multiline_df)

# COMMAND ----------

multiline_df.printSchema()

# COMMAND ----------

# MAGIC %fs ls /mnt/storagema

# COMMAND ----------

