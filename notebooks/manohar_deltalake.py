# Databricks notebook source
from delta.tables import *
DeltaTable.createOrReplace(spark)\
    .tableName("employee_demo")\
    .addColumn("emp_id","INT")\
    .addColumn("name","STRING")\
    .addColumn("gender","STRING")\
    .addColumn("salary","INT")\
    .addColumn("department","STRING")\
    .location("dbfs:/FileStore/manoharfile/delta")\
    .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee_demo

# COMMAND ----------

from pyspark.sql.types import IntegerType,StringType,StructType,StructField
data=[(101,"manohar","M",10000,"civil"),(102,"hari","M",12000,"cse"),(103,"madhu","M",10000,"mech"),(104,"manohar","M",10000,"civil"),(105,"vamsi","M",100000,"Medicine")]
schema = StructType([
    StructField("emp_id", IntegerType()),
    StructField("name", StringType()),
    StructField("gender", StringType()),
    StructField("salary", IntegerType()),
    StructField("department", StringType())
])


# COMMAND ----------

df=spark.createDataFrame(data=data,schema=schema)
display(df)

# COMMAND ----------

df.write.format("delta").mode("append").saveAsTable("employee_demo")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee_demo

# COMMAND ----------

data1=[(106,"jaggu","M",80000,"ece")]
schema1 = StructType([
    StructField("emp_id", IntegerType()),
    StructField("name", StringType()),
    StructField("gender", StringType()),
    StructField("salary", IntegerType()),
    StructField("department", StringType())
])
df1=spark.createDataFrame(data=data1,schema=schema1)
display(df1)

# COMMAND ----------

df1.write.insertInto("employee_demo",overwrite=False)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emplyoye_demo

# COMMAND ----------

df1.createOrReplaceTempView("delta_data")


# COMMAND ----------

# MAGIC %sql
# MAGIC insert into employee_demo
# MAGIC select * from delta_data

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee_demo

# COMMAND ----------

# MAGIC %md #####Delete method using sql

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from employee_demo where emp_id=106

# COMMAND ----------

# MAGIC %md ####Delete using spark sql

# COMMAND ----------


spark.sql("delete from employee_demo where emp_id==105")

# COMMAND ----------

# MAGIC %md #### Deleting using pyspark delta table instance

# COMMAND ----------

deltatableinstance=DeltaTable.forName(spark,"employee_demo")
deltatableinstance.delete("emp_id=103")

# COMMAND ----------

# MAGIC %md #### update method using table instance
# MAGIC

# COMMAND ----------

deltatableinstance.update(condition="name='manohar'",set={"salary":"15000"})
display(deltatableinstance.toDF())

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history employee_demo
# MAGIC

# COMMAND ----------

# MAGIC %md #### Merge(upsert)

# COMMAND ----------

schema2=schema = StructType([
    StructField("name", StringType(),False),
    StructField("city", StringType(), True),
    StructField("country", StringType(),True),
    StructField("contact_no", StringType(),True),
    StructField("emp_id", IntegerType(),False)
])
data2=[("kohli","delhi","india","630521456",1)]
df_merge=spark.createDataFrame(data=data2,schema=schema2)
display(df_merge)

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC create table dim_employee(emp_id INT,name STRING,city STRING,country STRING,contact_no String)

# COMMAND ----------

df_merge.createOrReplaceTempView('source_view')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from source_view

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into dim_employee as target
# MAGIC using source_view as src
# MAGIC on target.emp_id==src.emp_id
# MAGIC when matched then
# MAGIC update set
# MAGIC     target.name = src.name,
# MAGIC     target.city = src.city,
# MAGIC     target.country = src.country,
# MAGIC     target.contact_no = src.contact_no
# MAGIC when not matched then
# MAGIC   insert (emp_id, name, city, country, contact_no)
# MAGIC   values (src.emp_id, src.name, src.city, src.country, src.contact_no)
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history employee_demo

# COMMAND ----------

# MAGIC %md #### Time travel concept using version

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee_demo version as of 3

# COMMAND ----------

