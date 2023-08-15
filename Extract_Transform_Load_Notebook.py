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
