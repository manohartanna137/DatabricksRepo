# Databricks notebook source
from pyspark.sql.types import StructField,StructType,StringType,LongType,ArrayType
from pyspark.sql.functions import when,col,explode_outer 

# COMMAND ----------

# MAGIC %md #### Writing a function for creating of mount point

# COMMAND ----------


def mounting_layer_json(source,mountpoint,key,value):
    dbutils.fs.mount(
    source=source,
    mount_point= mountpoint,
    extra_configs={key:value})

# COMMAND ----------

# MAGIC %md #### calling the function and creating a mount storage name mountpointstorage

# COMMAND ----------


mounting_layer_json('wasbs://dataarchitecture@manstoragedemo.blob.core.windows.net/','/mnt/mountpointstorage','fs.azure.account.key.manstoragedemo.blob.core.windows.net','+tYtgZOyeg0s687h09bkDe2Vaoqt0ZUb0ZRAb8Ki8Sedxf5bSuae/JOSObRiGAMB1Cwmh7iY3jA0+AStm3dWoA==')

# COMMAND ----------

# MAGIC %md #### Writing a function to create a dataframe for reading a csv file

# COMMAND ----------

def read_csv_bronze(fileformat,path,header=True):
    df=spark.read.format(fileformat).option("header",header).load(path)
    return df

# COMMAND ----------

# MAGIC %md #### Reading the csv file from the bronze layer by calling function

# COMMAND ----------

df_sample_csv=read_csv_bronze("csv","dbfs:/mnt/mountpointstorage/Bronze/sample_csv_output.csv")
display(df_sample_csv)

# COMMAND ----------

# MAGIC %md #### Writing a function for reading json file

# COMMAND ----------

def read_json_bronze(fileformat,multiline,path):
    df=spark.read.format(fileformat).option("multiline",multiline).load(path)
    return df

# COMMAND ----------

# MAGIC %md #### Reading the json file from bronze layer

# COMMAND ----------

df_json=read_json_bronze("json","true","dbfs:/mnt/mountpointstorage/Bronze/json_output.json")
display(df_json)

# COMMAND ----------

# MAGIC %md #### Flattening the columns of the json file using explode outer

# COMMAND ----------


df_json_explode = df_json.withColumn("name",explode_outer(col("projects.name")))\
             .withColumn("status",explode_outer(col("projects.status"))).drop("projects")
display(df_json_explode)

# COMMAND ----------

# MAGIC %md #### Displaying the null values

# COMMAND ----------

 df_json_explode.filter(df_json_explode.department.isNull()).show()
 df_json_explode.filter(df_json_explode.id.isNull()).show()
 df_json_explode.filter(df_json_explode.salary.isNull()).show()
 df_json_explode.filter(df_json_explode.name.isNull()).show()
 df_json_explode.filter(df_json_explode.status.isNull()).show()

# COMMAND ----------

# MAGIC %md #### Function for column rename

# COMMAND ----------

def withcolumnrename(df,colname,newcolname):
    df_rename=df.withColumnRenamed(colname,newcolname)
    return df_rename

# COMMAND ----------

# MAGIC %md #### Renaming the column of json data frame

# COMMAND ----------

df_json_column_rename=withcolumnrename(df_json_explode,"name","projectname")
display(df_json_column_rename)

# COMMAND ----------

# MAGIC %md #### Function for imputation the null values

# COMMAND ----------

def impute_null_values(df,colname1,colname2,value2,value):
    df_null =df.na.fill(value,[colname1]).na.fill(value2,[colname2])
    return df_null

# COMMAND ----------



# COMMAND ----------

df_json_impute=impute_null_values(df_json_column_rename,"salary","projectname","0",value=0)
display(df_json_impute)

# COMMAND ----------

df_imputed_json=df_json_impute.na.fill('0',["status"])
display(df_imputed_json)

# COMMAND ----------

# MAGIC %md #### Imputation the null values of the csv file dataframe

# COMMAND ----------

df_imputed_csv=df_sample_csv.withColumn("city",when(df_sample_csv.city=="Null","0").\
    when(df_sample_csv.city.isNull(),"0").otherwise(df_sample_csv.city)).\
        na.fill("0",["city"])\
            .na.fill("0",["age"]).na.fill("0",["name"])
display(df_imputed_csv)

# COMMAND ----------

df_imputed_csv.filter(df_sample_csv.city=='Null').show()

# COMMAND ----------

# MAGIC %md #### Crosscheching the null values whether imputed or not

# COMMAND ----------

 df_imputed_json.filter(df_imputed_json.department.isNull()).show()
 df_imputed_json.filter(df_imputed_json.id.isNull()).show()
 df_imputed_json.filter(df_imputed_json.salary.isNull()).show()
 df_imputed_json.filter(df_imputed_json.projectname.isNull()).show()
 df_imputed_json.filter(df_imputed_json.status.isNull()).show()

# COMMAND ----------

# MAGIC %md #### Function for changing the column name from lower to upper case

# COMMAND ----------

def change_column_case(df):
    df_upper = df.toDF(*[name.upper() for name in df.columns])
    return(df_upper)

# COMMAND ----------

# MAGIC %md #### Dataframe for upper case columns

# COMMAND ----------

df_upper_csv=change_column_case(df_imputed_csv)
display(df_upper_csv)

# COMMAND ----------

df_upper_json=change_column_case(df_imputed_json)
display(df_upper_json)

# COMMAND ----------

def change_column_case(df):
    df_upper = df.toDF(*[name.lower() for name in df.columns])
    return(df_upper)

# COMMAND ----------

df_lower_csv=change_column_case(df_upper_csv)
df_lower_json=change_column_case(df_upper_json)
display(df_lower_csv)
display(df_lower_json)

# COMMAND ----------

# MAGIC %md #### Function for getting distinct values

# COMMAND ----------

def distinct_values_df(df):
    df_distinct=df.distinct()
    return df_distinct

# COMMAND ----------

df_csv_distinct=distinct_values_df(df_lower_csv)
display(df_csv_distinct)
df_json_distinct=distinct_values_df(df_lower_json)
display(df_json_distinct)

# COMMAND ----------

# MAGIC %md #### Writing the csv file in parquet format and moving it into silver path

# COMMAND ----------

df_csv_distinct.write.format("parquet").mode("append").save("/mnt/mountpointstorage/Silver/csv")

# COMMAND ----------

# MAGIC %md #### Writing the json file in parquet format and moving it into silver path

# COMMAND ----------

df_json_distinct.write.format("parquet").mode("append").save("/mnt/mountpointstorage/Silver/json")

# COMMAND ----------

# MAGIC %md #### Function for reading the file from the silver path layer

# COMMAND ----------

def read_from_silver(fileformat,path):
    df=spark.read.format(fileformat).load(path)
    return df


# COMMAND ----------

# MAGIC %md ####  reading the sample csv file from the silver path layer

# COMMAND ----------

df_silver_json=read_from_silver("parquet","/mnt/mountpointstorage/Silver/json")
display(df_silver_json)

# COMMAND ----------

# MAGIC %md ####reading the sample json file from the silver path layer

# COMMAND ----------

df_silver_csv=read_from_silver("parquet","/mnt/mountpointstorage/Silver/csv")
display(df_silver_csv)

# COMMAND ----------

# MAGIC %md #### Function for joining two data frames

# COMMAND ----------

def join_two_dataframes(df1,df2,colname,joinname):
    df_join=df1.join(df2,[colname],joinname)
    return df_join

# COMMAND ----------

# MAGIC %md #### Joining sample csv df and json sample df by using inner join

# COMMAND ----------

join_df=join_two_dataframes(df_silver_csv,df_silver_json,"id","inner")
display(join_df)

# COMMAND ----------

# MAGIC %md #### writing it into the delta format and saving it into the gold layer

# COMMAND ----------

join_df.write.format("delta").option("path","/mnt/mountpointstorage/Gold").saveAsTable("employee_info")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee_info

# COMMAND ----------

def delete_id(df,list_id,name):
    df_rm=df.filter(~col(name).isin(list_id))
    return df_rm

# COMMAND ----------

list_id=[31,40,7,15]
join_df=delete_id(join_df,list_id,"id")
display(join_df)

# COMMAND ----------

join_df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee_info

# COMMAND ----------

# MAGIC %md #### delete the records from id column

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from employee_info where id in (1,40,7,15)

# COMMAND ----------

# MAGIC %md #### querrying the table to see whether the table records got deleted or not

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee_info

# COMMAND ----------

# MAGIC %md #### Check the history of the table what operations are performed

# COMMAND ----------

# MAGIC %sql 
# MAGIC describe  history employee_info
# MAGIC

# COMMAND ----------

# MAGIC %md #### Restoreing the table table to version 0 to get the original table before doing delete operation

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE TABLE employee_info TO version as of 0

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee_info

# COMMAND ----------

