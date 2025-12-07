# Databricks notebook source
import pandas as pd
import numpy as np

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vision_dev.vision.monthly_forecast_3W_to_O9 limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table vision_dev.vision.monthly_forecast_3W_to_O9_new

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE vision_dev.vision.monthly_forecast_3W_to_O9_new AS
# MAGIC SELECT
# MAGIC     CAST(order_date AS STRING) AS order_date,
# MAGIC     CAST(dealer_id AS DOUBLE) AS dealer_id,
# MAGIC     CAST(country AS STRING) AS country,
# MAGIC     CAST(part_id AS STRING) AS part_id,
# MAGIC     CAST(order_qty AS DOUBLE) AS order_qty,
# MAGIC     CAST(type AS STRING) AS type,
# MAGIC     CAST(Material_Description AS STRING) AS Material_Description,
# MAGIC     CAST(industry AS STRING) AS industry,
# MAGIC     CAST(ABC_FMS AS STRING) AS ABC_FMS,
# MAGIC     CAST(run_date AS STRING) AS run_date
# MAGIC FROM vision_dev.vision.monthly_forecast_3W_to_O9;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vision_dev.vision.monthly_forecast_3W_to_O9_new limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table vision_dev.vision.IB_industry_country_spare_backup_3W

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE vision_dev.vision.IB_industry_country_spare_backup_3W AS
# MAGIC SELECT *
# MAGIC FROM vision_dev.vision.IB_industry_country_spare;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from vision_dev.vision.IB_industry_country_spare_backup_3W

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from vision_dev.vision.IB_industry_country_spare

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from vision_dev.vision.monthly_forecast_3W_to_O9_new

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vision_dev.vision.IB_industry_country_spare limit 10
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vision_dev.vision.monthly_forecast_3W_to_O9_new limit 10

# COMMAND ----------

spark.sql("""insert into vision_dev.vision.IB_industry_country_spare select order_date,dealer_id,country,part_id,order_qty,Material_Description,industry,ABC_FMS,run_date,type from vision_dev.vision.monthly_forecast_3W_to_O9_new""")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from vision_dev.vision.IB_industry_country_spare