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
password = "#######" #Enter your Password 
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


