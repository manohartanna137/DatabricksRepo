# Databricks notebook source
dbutils.fs.help('mount')

# COMMAND ----------

dbutils.fs.mount(
  source= 'wasbs://mountcontainer@manstoragedemo.blob.core.windows.net/',
  mount_point= '/mnt/manoharblobstorage',
  extra_configs={'fs.azure.account.key.manstoragedemo.blob.core.windows.net':'+tYtgZOyeg0s687h09bkDe2Vaoqt0ZUb0ZRAb8Ki8Sedxf5bSuae/JOSObRiGAMB1Cwmh7iY3jA0+AStm3dWoA=='})

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls '/mnt/manoharblobstorage'

# COMMAND ----------

df=spark.read.csv('/mnt/manoharblobstorage',inferSchema=True,header=True)
display(df)

# COMMAND ----------


