# Databricks notebook source
# MAGIC %run ./Users/hazarath540@gmail.com/dynamic_notebook $path="/FileStore/tables/emp.csv"  $name="emp_csv"

# COMMAND ----------

dbutils.notebook.run("./Users/hazarath540@gmail.com/dynamic_notebook",timeout_seconds=600,arguments={"path":"/FileStore/tables/emp_csv","name":"emp_csv"})

# COMMAND ----------

