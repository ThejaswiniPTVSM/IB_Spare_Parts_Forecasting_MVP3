# Databricks notebook source
# DBTITLE 1,Import required libraries
import pandas as pd
import numpy as np
import decimal
from datetime import datetime

from pyspark.sql import functions as F
from pyspark.sql.window import Window

#monthname='2025-10-03'
current_date = datetime.now()
monthname = current_date.replace(day=3).strftime('%Y-%m-%d')

# COMMAND ----------

# DBTITLE 1,Data Extraction
df = spark.sql(f"""
    SELECT Part_Id,DealerID_country, SUM(qty) AS qty, SUM(value_INR) AS value
    FROM (select MaterialNumber as part_id,CONCAT(DealerID, '_', country) AS DealerID_country, cast(BillingDate as date) as LeoDate, cast(YF2_InvoiceQuantity as int) qty, YF2_Invoice_ValueinINR as value_INR from oogib_cin.bumblebee.ib_parts_billing_report 
        WHERE BillingDate between (last_day(add_months(current_date, -7)) + 1) AND (last_day(add_months(current_date, -1))) and industry='3W' and Type = 'Parts' and country<>''
        GROUP BY all) group by all
""").toPandas()

# COMMAND ----------

# DBTITLE 1,Contribution Ratio Calculation
df_spark = spark.createDataFrame(df)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Compute total qty per Part_Id and start_of_month
df_spark = df_spark.withColumn(
    "total_qty", F.sum("qty").over(Window.partitionBy("Part_Id"))
)

# Calculate contribution percentage for each country within each Part_Id and start_of_month
df_spark = df_spark.withColumn(
    "contribution_percentage", (F.col("qty") / F.col("total_qty")) * 100
)

# Rank countries by contribution percentage within each Part_Id and start_of_month
window_spec = Window.partitionBy("Part_Id").orderBy(F.desc("contribution_percentage"))
df_spark = df_spark.withColumn("rank", F.row_number().over(window_spec))

# Compute cumulative contribution percentage per Part_Id and start_of_month
df_spark = df_spark.withColumn(
    "cumulative_contribution", F.sum("contribution_percentage").over(window_spec)
)

# Filter rows where cumulative contribution <= 99%
top_countries_df = df_spark.filter(F.col("cumulative_contribution") <= 99.5)

# Compute total contribution percentage per Part_Id and start_of_month
top_countries_df = top_countries_df.withColumn(
    "total_contribution", F.sum("contribution_percentage").over(Window.partitionBy("Part_Id"))
)

# Calculate the ratio of contribution per part per country
top_countries_df = top_countries_df.withColumn(
    "contribution_ratio", F.col("contribution_percentage") / F.col("total_contribution")
)

# Select necessary columns for the final output
final_df = top_countries_df.select(
    "Part_Id", "DealerID_country", "contribution_ratio"
)

final_df = final_df.withColumn("DealerID", F.split(F.col("DealerID_country"), "_")[0]) \
                               .withColumn("country", F.split(F.col("DealerID_country"), "_")[1]) \
                               .drop("DealerID_country")



# COMMAND ----------

# DBTITLE 1,Conversion to Pandas
final_df_spark=final_df.toPandas()

# COMMAND ----------

# DBTITLE 1,Forecast Data Extraction
df_forecast = spark.sql(f"""SELECT * from vision_dev.vision.monthly_plan_allocation_3W_base""")

# COMMAND ----------

# DBTITLE 1,Data Preview
df_forecast.head(5)

# COMMAND ----------

# DBTITLE 1,Data Preparation as per Output format
# Join the datasets on Part_Id and start_of_month
joined_df = df_forecast.join(final_df,on=["Part_Id"],how="inner")

# Calculate the country-level Actual_Qty and Forecast_Qty using contribution_ratio
result_df = joined_df.withColumn(
    "Forecast_Qty", F.col("Rounded_Qty") * F.col("contribution_ratio")
)

# Select and display the required columns
final_df = result_df.select(
    F.to_date("Forecast_month").alias("Forecast_month"), "DealerID", "country","industry","ABC_FMS","Part_Id", "Forecast_Qty", "Rounded_Qty"
)

# Round the Country_Actual_Qty and Country_Forecast_Qty
final_df = final_df.withColumn(
    "Forecast_Qty", F.round(F.col("Forecast_Qty"), 0)
)

# Drop rows where the country is null
final_df = final_df.filter(F.col("country").isNotNull())

# Display the final DataFrame
#display(final_df)

# COMMAND ----------

# DBTITLE 1,Data Preparation as per Output format
IB_Spares_3W=final_df.toPandas()

IB_Spares_3W['ABC_FMS'] = IB_Spares_3W['ABC_FMS'].replace({
    'AF': 'A_F',
    'BF': 'B_F',
    'CF': 'C_F',
    'BM': 'B_M',
    'CM': 'C_M',
    'AM': 'A_M',
    'BS': 'B_S',
    'AS': 'A_S',
    'CS': 'C_S'
})

IB_Spares_3W['type']='Forecast'
current_date = datetime.now()
run_date = current_date.replace(day=3).strftime('%Y-%m-%d')
IB_Spares_3W['run_date'] = run_date

#IB_Spares_3W['run_date']='2025-10-03'

IB_Spares_3W['DealerCode'] = IB_Spares_3W['DealerID']

Active_Parts=pd.read_csv('/Workspace/Users/thejaswini.p@tvsmotor.com/IB_Spare_Parts_MVP1/Input/Active_Parts_Master.csv', encoding='ISO-8859-1')
Active_Parts.rename(columns={'3W_Part_Id': 'Part_Id','Alternate_Part_Id':'Alternate_Part_No'}, inplace=True)
Active_Parts = Active_Parts[Active_Parts['Part_Id'].notnull()]

Active_Parts = Active_Parts[['Part_Id','Part_Description']]

IB_Spares_3W = pd.merge(IB_Spares_3W, Active_Parts, on='Part_Id', how='inner')

IB_Spares_3W.rename(columns={'Forecast_month':'order_date','DealerCode': 'dealer_id','Part_Id':'part_id','Country': 'country','Part_Description': 'Material_Description','Forecast_Qty':'order_qty'}, inplace=True)

IB_Spares_3W_to_O9 = IB_Spares_3W[['order_date','dealer_id','country','part_id','order_qty','type','Material_Description','industry','ABC_FMS','run_date']]

# COMMAND ----------

# DBTITLE 1,Output Preview
IB_Spares_3W_to_O9.head(5)

# COMMAND ----------

# DBTITLE 1,Write to Delta Table
from pyspark.sql.functions import col

# Cast order_date to string
spark_Plan_forecast_3W = spark.createDataFrame(IB_Spares_3W_to_O9)

# Write the DataFrame to a Delta table
spark_Plan_forecast_3W.write.mode("overwrite").format("delta").partitionBy("order_date").saveAsTable("vision_dev.vision.monthly_forecast_3W_to_O9")

# COMMAND ----------

# DBTITLE 1,SQL Output Check
# MAGIC %sql
# MAGIC select * from vision_dev.vision.monthly_forecast_3W_to_O9 limit 10

# COMMAND ----------

