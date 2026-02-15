# Databricks notebook source
dbutils.widgets.text("catalog_name", "City Travels", "Enter Catalog Name")
catalog_name = dbutils.widgets.get("catalog_name")
print(catalog_name)

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.a_bronze;")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.b_silver;")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.c_gold;")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP CATALOG city_travels_medallionsss CASCADE;
