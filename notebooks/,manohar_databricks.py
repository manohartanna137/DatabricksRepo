# Databricks notebook source
dbutils.fs.mkdirs("dbfs:/FileStore/manoharfile")

# COMMAND ----------

# MAGIC %sql
# MAGIC select "I'M running SQL"

# COMMAND ----------

dbutils.fs.ls("/databricks-datasets")


# COMMAND ----------

display(dbutils.fs.ls("/databricks-datasets"))


# COMMAND ----------

from pyspark.sql.functions import when,desc,col
from pyspark.sql.types import StructType,StructField,StringType,IntegerType

def read_csv(path,delim,value):
    df=spark.read.format('csv').option("header",value).option("delimitor",delim).option("inferSchema",value).load(path)
    return df

df1=read_csv("dbfs:/FileStore/manoharfile/Book4_1.csv",',',True)
df1.show()

# COMMAND ----------

df2=df1.withColumn('Department',when(df1.Department=='ds','gh').otherwise(df1.Department))
display(df2)

# COMMAND ----------

schema = StructType ([
             StructField("employee_name",StringType(),True),
             StructField("department",StringType(),True),
             StructField("salary",IntegerType(),True)
          ])

data = [
   ("James","Sales",3000),
   ("Micheal","Sales",4600),
   ("Robert","Sales",4100),
   ("Maria","Finance",3000),
   ("Raman","Finance",3000),
   ("Scott","Finance",3300),
   ("Jen","Finance",3900),
   ("Jeff","Marketing",3000),
   ("Kumar","Marketing",2000)
]


# COMMAND ----------


df_salary_details = spark.createDataFrame (data, schema)

display(df_salary_details)

# COMMAND ----------

df1=df_salary_details.orderBy(df_salary_details.salary.desc()).limit(1)
display(df1)

# COMMAND ----------

max_salary = df_salary_details.groupBy('department').agg(max('salary').alias("maxium_salary")).orderBy(desc('maxium_salary'))

max_salary.show()

# COMMAND ----------

from pyspark.sql.types import ArrayType
from pyspark.sql.functions import explode


data=[(1,[1,2]),(2,[3,4])]
schema=StructType([StructField(name='id',dataType=IntegerType()),\
                  StructField(name='numbers',dataType=ArrayType(IntegerType()))])

df=spark.createDataFrame(data,schema)
df.show()



df2=df.withColumn('first_number',df.numbers[0]).show()



data=[(1,2),(1,2)]
schema=StructType([StructField(name='num1',dataType=IntegerType()),\
                  StructField(name='num2',dataType=IntegerType())])

df=spark.createDataFrame(data,schema)
df.show()




df2=df.withColumn('Array',array(df.num1,df.num2)).show()




data=[(1,['azure','aws']),(1,['python','c++'])]
schema=StructType([StructField(name='id',dataType=IntegerType()),\
                  StructField(name='skills',dataType=ArrayType(StringType()))])

df=spark.createDataFrame(data,schema)
df.show()




df2=df.withColumn('Array',explode(df.skills)).show()

# COMMAND ----------

df=spark.createDataFrame(data,schema)
df.show()