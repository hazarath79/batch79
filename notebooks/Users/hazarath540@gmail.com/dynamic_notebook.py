# Databricks notebook source
dbutils.widgets.text('path','/FileStore/tables/emp.csv')
dbutils.widgets.text('name','emp_csv')
v_path= dbutils.widgets.get('path')
v_table_name=dbutils.widgets.get('name')

# COMMAND ----------

df=spark.read.option('nullValue','null').csv(v_path,header=True,inferSchema=True)

# COMMAND ----------

df=df.dropDuplicates().dropna(how='all')

# COMMAND ----------

df.write.format('delta').mode('append').saveAsTable(v_table_name)

# COMMAND ----------

dbutils.notebook.exit('success')

# COMMAND ----------

# MAGIC %sql
# MAGIC select*from emp_csv