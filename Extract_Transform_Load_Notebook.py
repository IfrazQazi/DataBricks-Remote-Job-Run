# Databricks notebook source
# MAGIC %md 
# MAGIC ### In this Notebook we are going to connect with Sql server DB Using python's pyodbc Library
# MAGIC ####  We will load the data into loading table 
# MAGIC #### then we will try to perfome some Transformation operation on Table
# MAGIC #### we will also going to perfom Upsert (Update insert) for increamental load into over target table
# MAGIC ### Which we will try execute using Job cluster from our local evnironment 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### lets import python's libraries

# COMMAND ----------

import pyodbc
import pandas as pd

# COMMAND ----------

# MAGIC %md
# MAGIC ##### lets also add Packages for MSSQL ODBC Driver 17 

# COMMAND ----------

# MAGIC %sh
# MAGIC curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
# MAGIC curl https://packages.microsoft.com/config/ubuntu/20.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
# MAGIC apt-get update
# MAGIC ACCEPT_EULA=Y apt-get install msodbcsql17

# COMMAND ----------

# MAGIC %md
# MAGIC #### Now lets connect with SQL Server to get the source data

# COMMAND ----------

# Some other example server values are
# server = 'myserver,port' # to specify an alternate port
server = 'tcp:learning-uc-ssvr.database.windows.net' 
database = 'learning-uc-dbs' 
username = 'ifraz' #user
password = '######' #Enter your Password 
# ENCRYPT defaults to yes starting in ODBC Driver 17. It's good to always specify ENCRYPT=yes on the client side to avoid MITM attacks.

cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';ENCRYPT=yes;UID='+username+';PWD='+ password)

# COMMAND ----------

## converting sql db to pandas df
df = pd.read_sql("select *  from dbo.ajiofashion", cnxn)

# COMMAND ----------

# lets check the data
df.head()

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### we successfully loaded data and created DataFrame

# COMMAND ----------

# MAGIC %md
# MAGIC #### lets create DDLs for data validation
# MAGIC ##### Creating Data Definition Language (DDL) statements in Databricks allows you to have more control and flexibility over the structure and properties of your SQL tables. While you can directly convert data into SQL tables in Databricks using DataFrame APIs, using DDLs provides several benefits:
# MAGIC
# MAGIC * Table Structure and Data Types: DDLs allow you to explicitly define the structure of your SQL table, including column names, data types, constraints, and indexes. This ensures consistency and accuracy in the schema of the table.
# MAGIC
# MAGIC * Data Validation and Transformation: DDLs provide the opportunity to validate and transform your data before it is loaded into the SQL table. You can perform data cleansing, type casting, and other transformations to ensure the data is of the desired quality.
# MAGIC
# MAGIC * Control Over Storage Properties: With DDLs, you can specify storage-related properties such as file format, compression, partitioning, and clustering. This enables you to optimize data storage and query performance based on your specific use case.
# MAGIC
# MAGIC * Data Partitioning: DDLs allow you to define how the data should be partitioned in the SQL table. Partitioning can significantly improve query performance, especially when dealing with large datasets.
# MAGIC
# MAGIC * Schema Evolution: Using DDLs, you can easily modify the schema of your SQL tables over time without affecting the data. This is particularly useful when you need to add new columns, change data types, or update constraints.
# MAGIC
# MAGIC * Customization: DDLs provide a way to customize table properties and behavior that may not be available through direct DataFrame-to-table conversion.

# COMMAND ----------

# MAGIC %md
# MAGIC #### But Before creating DDL we have to connect with the storage location(adls gen2)
# MAGIC ##### I am using Access Token you can use anything you want like (Service Principal,App Registration) 

# COMMAND ----------

spark.conf.set("fs.azure.account.key.adlsgen2strgaccnt.dfs.core.windows.net"
               ,"b101lF9Dh5Vwp9vg2NT2UGdJjRnOhk2SH8o655wwOszpwraQaeOZpjO+Q/FTGFGwaDjWesvabRTm+AStO8i/ag==")


# COMMAND ----------

#lets check the container
display(dbutils.fs.ls("abfss://populationadls@adlsgen2strgaccnt.dfs.core.windows.net/population/"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Now we can use this location to store our data 

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Below command is used to create new schema if its already present then you don't need to create it again

# COMMAND ----------

# %sql
# Create SCHEMA ajio_detail

# COMMAND ----------

# MAGIC %md
# MAGIC ###### below command is to remove file or created table from the file path incase when you accedentaly created the incorrect table with column is not valid or some other changes you want to do with that table

# COMMAND ----------

# %fs
# rm -r 'abfss://populationadls@adlsgen2strgaccnt.dfs.core.windows.net/population/ajio_fashion'

# COMMAND ----------

# MAGIC %md
# MAGIC ###### drop table command is used to drop table. # 
# MAGIC ###### need to be careful when we use this command

# COMMAND ----------

# %sql
# drop table ajio_detail.ajio_fashion

# COMMAND ----------

# MAGIC %md
# MAGIC ##### i have created the ddl in the same notebook but you can create it in different notebook so that when you run entire notebook you don't have recreate the tables again. it will be store in to hive metastore
# MAGIC ##### and if you try to recreate table without droping it than it will throw an error table is already present 

# COMMAND ----------

# MAGIC %sql
# MAGIC create table ajio_detail.ajio_fashion(
# MAGIC Product_URL String,
# MAGIC Brand String,
# MAGIC Description String,
# MAGIC Id_Product decimal(38,17),
# MAGIC URL_image String,
# MAGIC Category_by_gender String,
# MAGIC Discount_Price_in_Rs int,
# MAGIC Original_Price_in_Rs int,
# MAGIC Color String
# MAGIC )
# MAGIC using parquet
# MAGIC location "abfss://populationadls@adlsgen2strgaccnt.dfs.core.windows.net/population/ajio_fashion"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### above i have use parquet to store the data into the parquet format you can also use json,and delta or other formats.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- drop table ajio_detail.fact_ajio_fashion 

# COMMAND ----------

# MAGIC %sql
# MAGIC create table ajio_detail.fact_ajio_fashion(
# MAGIC Product_URL_Id String,
# MAGIC Brand_Name String,
# MAGIC Description String,
# MAGIC Product_Id decimal(38,17),
# MAGIC URL_image String,
# MAGIC Category_By_Gender_Name String,
# MAGIC Discount_Price_In_Rs_Amount int,
# MAGIC Original_Price_In_Rs_Amount int,
# MAGIC Color_Name String
# MAGIC )
# MAGIC using delta
# MAGIC location "abfss://populationadls@adlsgen2strgaccnt.dfs.core.windows.net/population/fact_ajio_fashion"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### as we seen ealier we can use any format so here i have used the delta format. It is ok to use defferent format it depends on the requirements of the project.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### lets create spark dataframe from pandas dataframe

# COMMAND ----------

sql_table=spark.createDataFrame(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### now lets create Temporary View so that we can move data from temporary View to loading table

# COMMAND ----------

sql_table.createOrReplaceTempView("temp_sql_table")

# COMMAND ----------

# MAGIC %md
# MAGIC ###### lets load the data from View to Loading table

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into ajio_detail.ajio_fashion SELECT DISTINCT * FROM temp_sql_table;
# MAGIC Drop View temp_sql_table;

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### in above insert query i have selected only distinct value from temp_sql_table(Temporary View) so that i will not load duplicate rows to loading table

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Lets now create Update insert Query or Incremental query for our target table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW ajio_fashion_tempV (  --- Creating Temporary View which containt non duplicated rows from loading table
# MAGIC     Product_URL,      
# MAGIC        Brand,
# MAGIC        Description,
# MAGIC        Id_Product ,
# MAGIC        URL_image,
# MAGIC        Category_by_gender,
# MAGIC        Discount_Price_in_Rs,
# MAGIC        Original_Price_in_Rs,
# MAGIC        Color
# MAGIC ) AS
# MAGIC SELECT Distinct Product_URL,
# MAGIC        Brand,
# MAGIC        Description,
# MAGIC        Id_Product ,
# MAGIC        URL_image,
# MAGIC        Category_by_gender,
# MAGIC        Discount_Price_in_Rs,
# MAGIC        Original_Price_in_Rs,
# MAGIC        Color
# MAGIC FROM ajio_detail.ajio_fashion;
# MAGIC --- Now lets create Merge query 
# MAGIC MERGE INTO ajio_detail.fact_ajio_fashion AS target
# MAGIC USING ajio_fashion_tempV AS source
# MAGIC ON target.URL_image = source.URL_image  -- this column containt unique values so i am using to match the columns
# MAGIC WHEN MATCHED THEN UPDATE                -- If values are matching than it will just update the table with new value
# MAGIC   SET target.Product_URL_Id = source.Product_URL,
# MAGIC       target.Brand_Name = source.Brand,
# MAGIC       target.Description = source.Description,
# MAGIC       target.Product_Id = source.Id_Product,
# MAGIC       target.URL_image = source.URL_image,
# MAGIC       target.Category_By_Gender_Name = source.Category_by_gender,
# MAGIC       target.Discount_Price_In_Rs_Amount = source.Discount_Price_in_Rs,
# MAGIC       target.Original_Price_In_Rs_Amount = source.Original_Price_in_Rs,
# MAGIC       target.Color_Name = source.Color
# MAGIC                                             -- If it doesn't match than it will just add it into the table
# MAGIC WHEN NOT MATCHED THEN INSERT (Product_URL_Id,
# MAGIC                               Brand_Name,
# MAGIC                               Description,
# MAGIC                               Product_Id,
# MAGIC                               URL_image,
# MAGIC                               Category_By_Gender_Name,
# MAGIC                               Discount_Price_In_Rs_Amount ,
# MAGIC                               Original_Price_In_Rs_Amount ,
# MAGIC                               Color_Name)
# MAGIC   VALUES (source.Product_URL,
# MAGIC           source.Brand,
# MAGIC           source.Description,
# MAGIC           source.Id_Product,
# MAGIC           source.URL_image,
# MAGIC           source.Category_by_gender,
# MAGIC           source.Discount_Price_in_Rs,
# MAGIC           source.Original_Price_in_Rs,
# MAGIC           source.Color);
# MAGIC Drop VIEW ajio_fashion_tempV;
# MAGIC SELECT COUNT(*) FROM ajio_detail.fact_ajio_fashion; -- This will display the count after duplicates are removed
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### so now we have done process from loading data from source to loading and performing tarnsformation on it.
# MAGIC ###### now this data is ready to push into the final target table. 

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from ajio_detail.fact_ajio_fashion

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table ajio_detail.fact_ajio_fashion
