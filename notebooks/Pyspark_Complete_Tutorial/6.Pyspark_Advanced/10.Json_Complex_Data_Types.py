# Databricks notebook source
# MAGIC %md
# MAGIC # Transforming Complex Data Types in Spark SQL
# MAGIC
# MAGIC In this notebook we're going to go through some data transformation examples using Spark SQL. Spark SQL supports many
# MAGIC built-in transformation functions in the module `pyspark.sql.functions` therefore we will start off by importing that.

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# Convenience function for turning JSON strings into DataFrames.
def jsonToDataFrame(json, schema=None):
  # SparkSessions are available with  
  reader = spark.read
  if schema:
    reader.schema(schema)
  return reader.json(sc.parallelize([json]))

# COMMAND ----------

nested_df = spark.read.json(sc.parallelize(["""
{
  "field1": 1, 
  "field2": 2, 
  "nested_array":{
     "nested_field1": 3,
     "nested_field2": 4
  }
}
"""]))
print(nested_df.printSchema())
flat_df = nested_df.select("field1", "field2", "nested_array.*")
print(flat_df.printSchema())
flat_df.show()


# COMMAND ----------

display(nested_df.select("nested_array.nested_field1"))

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Selecting from nested columns</b> - Dots (`"."`) can be used to access nested columns for structs and maps.

# COMMAND ----------

# Using a struct
from pyspark.sql.types import *
schema = StructType().add("a", StructType().add("b", IntegerType()))
                          
events = jsonToDataFrame("""{ "a": { "b": 1 }} """, schema)
print(events.printSchema())
display(events.select("a.b"))

# COMMAND ----------

# Using a map
schema = StructType().add("a", MapType(StringType(), IntegerType()))
                          
events = jsonToDataFrame("""{  "a": { "b": 1  }}""", schema)
print(events.printSchema())
display(events.select("a.b"))

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Flattening structs</b> - A star (`"*"`) can be used to select all of the subfields in a struct.

# COMMAND ----------

events = jsonToDataFrame("""{  "a": { "b": 1, "c": 2  }} """)

display(events.select("a.*"))

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Nesting columns</b> - The `struct()` function or just parentheses in SQL can be used to create a new struct.

# COMMAND ----------

events = jsonToDataFrame("""{  "a": 1,  "b": 2,  "c": 3}""")
events.printSchema()
events.select(struct(col("a").alias("y")).alias("x")).printSchema()
display(events.select(struct(col("a").alias("y")).alias("x")))


# COMMAND ----------

# MAGIC %md
# MAGIC <b>Nesting all columns</b> - The star (`"*"`) can also be used to include all columns in a nested struct.

# COMMAND ----------

events = jsonToDataFrame("""{  "a": 1,  "b": 2}""")

display(events.select(struct("*").alias("x")))

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Selecting a single array or map element</b> - `getItem()` or square brackets (i.e. `[ ]`) can be used to select a single element out of an array or a map.

# COMMAND ----------

events = jsonToDataFrame("""{  "a": [1, 2]}""")

display(events.select(col("a").getItem(0).alias("x")))

# COMMAND ----------

# Using a map
schema = StructType().add("a", MapType(StringType(), IntegerType()))

events = jsonToDataFrame("""{  "a": {    "b": 1  }}""", schema)

display(events.select(col("a").getItem("b").alias("x")))

# COMMAND ----------

events.select(explode("a")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Creating a row for each array or map element</b> - `explode()` can be used to create a new row for each element in an array or each key-value pair.  This is similar to `LATERAL VIEW EXPLODE` in HiveQL.

# COMMAND ----------

events = jsonToDataFrame("""{  "a": [1, 2]}""")

display(events.select(explode("a").alias("x")))

# COMMAND ----------

# Using a map
schema = StructType().add("a", MapType(StringType(), IntegerType()))

events = jsonToDataFrame("""{  "a": {    "b": 1,    "c": 2  }}""", schema)

display(events.select(explode("a").alias("x", "y")))

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Collecting multiple rows into an array</b> - `collect_list()` and `collect_set()` can be used to aggregate items into an array.

# COMMAND ----------

events = jsonToDataFrame("""[{ "x": 1 }, { "x": 2 }]""")

display(events.select(collect_list("x").alias("x")))

# COMMAND ----------

# using an aggregation
events = jsonToDataFrame("""[{ "x": 1, "y": "a" }, { "x": 2, "y": "b" }]""")

display(events.groupBy("y").agg(collect_list("x").alias("x")))

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Selecting one field from each item in an array</b> - when you use dot notation on an array we return a new array where that field has been selected from each array element.

# COMMAND ----------

events = jsonToDataFrame("""{  "a": [    {"b": 1},    {"b": 2}  ]}""")

display(events.select("a.b"))

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Convert a group of columns to json</b> - `to_json()` can be used to turn structs into json strings. This method is particularly useful when you would like to re-encode multiple columns into a single one when writing data out to Kafka. This method is not presently available in SQL.

# COMMAND ----------

events = jsonToDataFrame("""{  "a": {    "b": 1  }}""")

display(events.select(to_json("a").alias("c")))

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Parse a column containing json</b> - `from_json()` can be used to turn a string column with json data into a struct. Then you may flatten the struct as described above to have individual columns. This method is not presently available in SQL. 
# MAGIC **This method is available since Spark 2.1**

# COMMAND ----------

events = jsonToDataFrame("""{  "a": "{\\"b\\":1}"}""")

schema = StructType().add("b", IntegerType())
display(events.select(from_json("a", schema).alias("c")))

# COMMAND ----------

# MAGIC %md
# MAGIC Sometimes you may want to leave a part of the JSON string still as JSON to avoid too much complexity in your schema.

# COMMAND ----------

events = jsonToDataFrame("""{  "a": "{\\"b\\":{\\"x\\":1,\\"y\\":{\\"z\\":2}}}"}""")

schema = StructType().add("b", StructType().add("x", IntegerType())
                            .add("y", StringType()))
display(events.select(from_json("a", schema).alias("c")))

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Parse a set of fields from a column containing json</b> - `json_tuple()` can be used to extract a fields available in a string column with json data.

# COMMAND ----------

events = jsonToDataFrame("""{  "a": "{\\"b\\":1}"}""")

display(events.select(json_tuple("a", "b").alias("c")))

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Parse a well formed string column</b> - `regexp_extract()` can be used to parse strings using regular expressions.

# COMMAND ----------

events = jsonToDataFrame("""[{ "a": "x: 1" }, { "a": "y: 2" }]""")

display(events.select(regexp_extract("a", "([a-z]):", 1).alias("c")))

# COMMAND ----------

import pyspark
from pyspark.sql import SparkSession

arrayData = [
        ('Raj',['Java','Scala'],{'hair':'black','eye':'brown'}),
        ('Mahesh',['Spark','Java',None],{'hair':'brown','eye':None}),
        ('Ravi',['CSharp',''],{'hair':'red','eye':''}),
        ('Sridhar',None,None),
        ('Prasad',['1','2'],{})]
df = spark.createDataFrame(data=arrayData, schema=['name','knownLanguages','properties'])
df.printSchema()
df.show()

from pyspark.sql.functions import explode
df2 = df.select(df.name,explode(df.knownLanguages))
df2.printSchema()
df2.show()

# COMMAND ----------

from pyspark.sql.functions import explode
df3 = df.select(df.name,explode(df.properties))
df3.printSchema()
df3.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### explode_outer
# MAGIC * Returns a new row for each element in the given array or map. 
# MAGIC * Unlike explode, if the array/map is null or empty then null is produced. 
# MAGIC * Uses the default column name col for elements in the array and key and value for elements in the map unless specified otherwise.

# COMMAND ----------

from pyspark.sql.functions import explode_outer
print(""" with array explode""")
df.select(df.name,explode(df.knownLanguages)).show()
print(""" with map explode""")
df.select(df.name,explode(df.properties)).show()
print(""" with array explode_outer""")
df.select(df.name,explode_outer(df.knownLanguages)).show()
print(""" with map explode_outer""")
df.select(df.name,explode_outer(df.properties)).show()


# COMMAND ----------

# MAGIC %md
# MAGIC ### posexplode
# MAGIC * Returns a new row for each element with position in the given array or map. 
# MAGIC * Uses the default column name pos for position, and col for elements in the array and key and value for elements in the map unless specified otherwise.

# COMMAND ----------

from pyspark.sql.functions import posexplode
print(""" with array """)
df.select(df.name,posexplode(df.knownLanguages)).show()
print(""" with map """)
df.select(df.name,posexplode(df.properties)).show()


# COMMAND ----------

# MAGIC %md
# MAGIC ### posexplode_outer
# MAGIC * Returns a new row for each element with position in the given array or map. Unlike posexplode, if the array/map is null or empty then the row (null, null) is produced. 
# MAGIC * Uses the default column name pos for position, and col for elements in the array and key and value for elements in the map unless specified otherwise.

# COMMAND ----------

from pyspark.sql.functions import posexplode_outer
print(""" with array explode """)
df.select("name",explode("knownLanguages")).show()
print(""" with map explode """)
df.select(df.name,explode(df.properties)).show()
print(""" with array posexplode_outer""")
df.select("name",posexplode_outer("knownLanguages")).show()
print(""" with map posexplode_outer""")
df.select(df.name,posexplode_outer(df.properties)).show()

# COMMAND ----------

from pyspark.sql.functions import explode, col


# COMMAND ----------

source_json = """
{
    "persons": [
        {
            "name": "John",
            "age": 30,
            "cars": [
                {
                    "name": "Ford",
                    "models": [
                        "Fiesta",
                        "Focus",
                        "Mustang"
                    ]
                },
                {
                    "name": "BMW",
                    "models": [
                        "320",
                        "X3",
                        "X5"
                    ]
                }
            ]
        },
        {
            "name": "Peter",
            "age": 46,
            "cars": [
                {
                    "name": "Huyndai",
                    "models": [
                        "i10",
                        "i30"
                    ]
                },
                {
                    "name": "Mercedes",
                    "models": [
                        "E320",
                        "E63 AMG"
                    ]
                }
            ]
        }
    ]
}
"""

# COMMAND ----------

dbutils.fs.rm('/FileStore/tables/multiline_nested_json.json')

# COMMAND ----------

dbutils.fs.put('/FileStore/tables/multiline_nested_json.json',source_json,True)

# COMMAND ----------

source_df = spark.read.option("multiline", "true").json("/FileStore/tables/multiline_nested_json.json")
display(source_df)
source_df.schema

# COMMAND ----------

source_df.printSchema()

# COMMAND ----------

schema = StructType(List(StructField(persons,ArrayType(StructType(List(StructField(age,LongType,true),
                                                                       StructField(cars,ArrayType(StructType(List(StructField(models,ArrayType(StringType,true),true),
                                                                                                                  StructField(name,StringType,true))),true),true),
                                                                       StructField(name,StringType,true))),true),true)))

# COMMAND ----------

# Explode all persons into different rows
from pyspark.sql.functions import *
persons = source_df.select(explode("persons").alias("persons"))
display(persons)
          
            

# COMMAND ----------

# Explode all car brands into different rows
persons_cars = persons.select(
   col("persons.name").alias("persons_name")
 , col("persons.age").alias("persons_age")
 , explode("persons.cars").alias("persons_cars_brands")
 , col("persons_cars_brands.name").alias("persons_cars_brand")
)
display(persons_cars)

# COMMAND ----------

# Explode all car models into different rows
persons_cars_models = persons_cars.select(
   col("persons_name")
 , col("persons_age")
 , col("persons_cars_brand")
 , explode("persons_cars_brands.models").alias("persons_cars_model")
)
display(persons_cars_models)

# COMMAND ----------

persons_cars_models.write.format('delta').mode('append').saveAsTable('delta.customer_cars')


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.customer_cars

# COMMAND ----------

#All imports go here
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, ArrayType  
    

# COMMAND ----------

#Read data
#This example reads data from JSONs
#However, dataframe can be loaded from any input format
dataFrame = sqlContext.read.option("multiline","true").json("dbfs:/FileStore/tables/sample_json.json")
display(dataFrame)

# COMMAND ----------

from pyspark.sql.functions import explode
dataFrame.select(explode("goods.orders").alias("orders"),"goods.orders").show(truncate=False)

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql import functions as f

# COMMAND ----------

# MAGIC %md
# MAGIC #### Flattening nested json file into table format using below two functions

# COMMAND ----------

def func_flatten_array_structs(nested_df):
  """
  this function we can use for identifying nested data types like  ARRAY and STRUCT type fields and 
  Creating individual datatypes with underscore _ 
  input Param:  Pyspark Dataframe
  
  """
    # Creating python list to store dataframe metadata
  stack = [((), nested_df)]
    # Creating empty python list for storing identified nested data type columns information
  columns = []

  while len(stack) > 0:
      # Removing latest or recently added item (dataframe schema) and returning into df variable  
      parents, df = stack.pop()
      # Reading data types one by one using loop and validating array type and getting those  
      array_cols = [  c[0]  for c in df.dtypes   if c[1][:5] == "array"  ]
      
      # 
      flat_cols = [  f.col(".".join(parents + (c[0],))).alias("_".join(parents + (c[0],)))
            for c in df.dtypes
            if c[1][:6] != "struct"   ]
      
      nested_cols = [  c[0]   for c in df.dtypes if c[1][:6] == "struct"   ]
      # appending all columns
      
      columns.extend(flat_cols)
      #Reading  nested columns and appending into stack list
      
      for nested_col in nested_cols:
            projected_df = df.select(nested_col + ".*")
            stack.append((parents + (nested_col,), projected_df))
            
      
      
  return nested_df.select(columns)

# COMMAND ----------

def func_flatten_json_df(df):
  """
  Using this function reading all nested columns and converting into normal columns.
  """
    # getting no of array columns for looping
  array_cols = [  c[0]   for c in df.dtypes  if c[1][:5] == "array"   ]
    
  while len(array_cols) > 0:
        
      for array_col in array_cols:            
        cols_to_select = [x for x in df.columns if x != array_col ] 
        # Using Explode function flattening the data
        # explode - will ignore null records
        # explode_outer - will give null rows as well
        df = df.withColumn(array_col, f.explode_outer(f.col(array_col)))
      # calling above function to update latest fields. 
      
      df = func_flatten_array_structs(df)
      
      # reducing number based latest updated columns
      array_cols = [   c[0]  for c in df.dtypes    if c[1][:5] == "array"     ]
        
  return df



# COMMAND ----------

import pyspark.sql.functions as f
new_df = func_flatten_json_df(source_df)


# COMMAND ----------

from pyspark.sql import DataFrame
dataframes = [k for k,v in globals().items() if isinstance(v,DataFrame) ]

# COMMAND ----------

display(new_df)

# COMMAND ----------

display(new_df)

# COMMAND ----------

display(source_df)

# COMMAND ----------

#Read data
#This example reads data from JSONs
#However, dataframe can be loaded from any input format
df = spark.read.option("multiline","true").json("dbfs:/FileStore/tables/sample_json.json")
display(df)

# COMMAND ----------

df = spark.read.option("multiline","true").json("dbfs:/FileStore/tables/sample_json.json")

# COMMAND ----------

import pyspark.sql.functions as f
df = func_flatten_json_df(df)

# COMMAND ----------

display(df)

# COMMAND ----------

# Calling flatten_array_struct_df function
import pyspark.sql.functions as f
df_output = func_flatten_json_df(df)

# COMMAND ----------

display(df_output)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Using Scala extracting nested json file

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.functions.{split, sum,col}
# MAGIC import org.apache.spark.sql.{DataFrame, SaveMode, functions}
# MAGIC import org.apache.spark.sql.types._
# MAGIC
# MAGIC val json_location="dbfs:/FileStore/tables/sample_json.json"
# MAGIC  //Ideally this should not be given in my case.
# MAGIC val jsonFullDFSchemaString = spark.read.option("multiline","true").json(json_location).schema.json;
# MAGIC // changing values to reportData
# MAGIC val jsonFullDFSchemaStructType = DataType.fromJson(jsonFullDFSchemaString).asInstanceOf[StructType]
# MAGIC val df = spark.read.option("multiline","true").schema(jsonFullDFSchemaStructType).json(json_location);
# MAGIC display(df)

# COMMAND ----------

# MAGIC %scala
# MAGIC def flattenDataframe(df: DataFrame): DataFrame = {
# MAGIC     //getting all the fields from schema
# MAGIC     val fields = df.schema.fields
# MAGIC     val fieldNames = fields.map(x => x.name)
# MAGIC     //length shows the number of fields inside dataframe
# MAGIC     val length = fields.length
# MAGIC     for (i <- 0 to fields.length - 1) {
# MAGIC       val field = fields(i)
# MAGIC       val fieldtype = field.dataType
# MAGIC       val fieldName = field.name
# MAGIC       fieldtype match {
# MAGIC         case arrayType: ArrayType =>
# MAGIC           val fieldName1 = fieldName
# MAGIC           val fieldNamesExcludingArray = fieldNames.filter(_ != fieldName1)
# MAGIC           val fieldNamesAndExplode = fieldNamesExcludingArray ++ Array(s"explode_outer($fieldName1) as $fieldName1")
# MAGIC           //val fieldNamesToSelect = (fieldNamesExcludingArray ++ Array(s"$fieldName1.*"))
# MAGIC           val explodedDf = df.selectExpr(fieldNamesAndExplode: _*)
# MAGIC           return flattenDataframe(explodedDf)
# MAGIC
# MAGIC         case structType: StructType =>
# MAGIC           val childFieldnames = structType.fieldNames.map(childname => fieldName + "." + childname)
# MAGIC           val newfieldNames = fieldNames.filter(_ != fieldName) ++ childFieldnames
# MAGIC           val renamedcols = newfieldNames.map(x => (col(x.toString()).as(x.toString().replace(".", "_").replace("$", "_").replace("__", "_").replace(" ", "").replace("-", ""))))
# MAGIC           val explodedf = df.select(renamedcols: _*)
# MAGIC           return flattenDataframe(explodedf)
# MAGIC         case _ =>
# MAGIC       }
# MAGIC     }
# MAGIC     df
# MAGIC   }

# COMMAND ----------

# MAGIC %scala
# MAGIC val tableSchemaDF = flattenDataframe(df)
# MAGIC display(tableSchemaDF)

# COMMAND ----------

rdd = sc.parallelize(["""
{"name":"Company1","location":"Seattle", "satellites": ["New York"],
  "goods":{"trade":false, "customers":["store1", "store2"],
  "orders":[
      {"orderId":4,"orderTotal":123.34,"shipped":{"orderItems":[{"itemName":"Laptop","itemQty":20},{"itemName":"Charger","itemQty":3}]}},
      {"orderId":5,"orderTotal":343.24,"shipped":{"orderItems":[{"itemName":"Chair","itemQty":4},{"itemName":"Lamp","itemQty":2}]}}
    ]}},
{"name": "Company2", "location": "Bellevue",
  "goods": {"trade": true, "customers":["Bank"], "orders": [{"orderId": 4, "orderTotal": 123.34}]}},
{"name":"MSFT","location":"Redmond", "satellites": ["Bay Area", "Shanghai"],
  "goods": {
    "trade":true, "customers":["government", "distributer", "retail"],
    "orders":[
        {"orderId":1,"orderTotal":123.34,"shipped":{"orderItems":[{"itemName":"Laptop","itemQty":20},{"itemName":"Charger","itemQty":2}]}},
        {"orderId":2,"orderTotal":323.34,"shipped":{"orderItems":[{"itemName":"Mice","itemQty":2},{"itemName":"Keyboard","itemQty":1}]}}
    ]}},
{"name": "Company3", "location": "Kirkland"}"""])

# COMMAND ----------

dff = spark.read.option("multiLine","true").json(rdd)
display(dff)

# COMMAND ----------

dbutils.fs.put("/tmp/json.json","""[{"name":"MSFT","location":"Redmond", "satellites": ["Bay Area", "Shanghai"],
  "goods": {
    "trade":true, "customers":["government", "distributer", "retail"],
    "orders":[
        {"orderId":1,"orderTotal":123.34,"shipped":{"orderItems":[{"itemName":"Laptop","itemQty":20},{"itemName":"Charger","itemQty":2}]}},
        {"orderId":2,"orderTotal":323.34,"shipped":{"orderItems":[{"itemName":"Mice","itemQty":2},{"itemName":"Keyboard","itemQty":1}]}}
    ]}},
{"name":"Company1","location":"Seattle", "satellites": ["New York"],
  "goods":{"trade":false, "customers":["store1", "store2"],
  "orders":[
      {"orderId":4,"orderTotal":123.34,"shipped":{"orderItems":[{"itemName":"Laptop","itemQty":20},{"itemName":"Charger","itemQty":3}]}},
      {"orderId":5,"orderTotal":343.24,"shipped":{"orderItems":[{"itemName":"Chair","itemQty":4},{"itemName":"Lamp","itemQty":2}]}}
    ]}},
{"name": "Company2", "location": "Bellevue",
  "goods": {"trade": true, "customers":["Bank"], "orders": [{"orderId": 4, "orderTotal": 123.34}]}},
{"name": "Company3", "location": "Kirkland"}]""",True)

# COMMAND ----------

df_json = spark.read.option("multiline","true").json("dbfs:/FileStore/tables/sample_json-2.json")
display(df_json)

# COMMAND ----------

