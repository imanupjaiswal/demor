# Databricks notebook source
dbutils.fs.ls('dbfs:/FileStore/tables/anup/archive/')

# COMMAND ----------

source = 'dbfs:/FileStore/tables/anup/stage/'
spark.conf.set('spark.databricks.delta.formatCheck.enabled', 'false')
spark.conf.set('spark.databricks.delta.schema.autoMerge.enabled', 'true')
df = spark.read.format('csv').option('header', 'true').load(source)

df.write.format('delta').mode('overwrite').saveAsTable('hive_metastore.anup_jaiswal_8ev2_da_asp.city')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from hive_metastore.anup_jaiswal_8ev2_da_asp.city;

# COMMAND ----------

source = 'dbfs:/FileStore/tables/anup/stage'
target = 'dbfs:/Filestore/tables/anup/archive'
delta_tab = 'samples.default.city_data'








