# Databricks notebook source
# DBTITLE 1,Import required libraries
import pandas as pd
from pyspark.sql.functions import col, desc, sum, when
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Query and aggregate 3W parts billing data
df = spark.sql(f"""SELECT Part_Id as Part_no, SUM(qty) AS qty, SUM(value_INR) AS value
    FROM (select MaterialNumber as part_id, cast(BillingDate as date) as LeoDate, cast(YF2_InvoiceQuantity as int) qty, YF2_Invoice_ValueinINR as value_INR from oogib_cin.bumblebee.ib_parts_billing_report 
        WHERE 
        BillingDate between (last_day(add_months(current_date, -13)) + 1) AND (last_day(add_months(current_date, -1))) and 
        industry='3W' and Type = 'Parts' 
        GROUP BY all) group by all
""")

# COMMAND ----------

# DBTITLE 1,Calculate total value and quantity for all parts
# Calculate total value and quantity for all parts
total_value = df.agg(sum("value").alias("total_value")).collect()[0]["total_value"]
total_qty = df.agg(sum("qty").alias("total_qty")).collect()[0]["total_qty"]

# Calculate contribution percentage for value and quantity per part
contribution_df = df.groupBy("Part_no").agg(
    (sum("value") / total_value * 100).alias("value_contribution_pct"),
    (sum("qty") / total_qty * 100).alias("qty_contribution_pct")
)

# Show the result
#contribution_df.display()
contribution_df.createOrReplaceTempView("contribution_df_view")

# COMMAND ----------

# DBTITLE 1,Summarize the total value and quantity contribution percentages
# MAGIC %sql
# MAGIC select sum(value_contribution_pct),sum(qty_contribution_pct) from contribution_df_view

# COMMAND ----------

# DBTITLE 1,Count the number of unique parts
# MAGIC %sql
# MAGIC select count(distinct(Part_No)) from contribution_df_view

# COMMAND ----------

# DBTITLE 1,Calculate cumulative contribution percentages
df = contribution_df
# Calculate cumulative sums
df = df.withColumn("value_contribution_cumsum", sum(col("value_contribution_pct")).over(Window.orderBy(desc("value_contribution_pct"))))
df = df.withColumn("qty_contribution_cumsum", sum(col("qty_contribution_pct")).over(Window.orderBy(desc("qty_contribution_pct"))))

# Add category columns
df = df.withColumn("value_contribution_pct_category",
                   when((col("value_contribution_cumsum") >= 0) & (col("value_contribution_cumsum") <= 80), "A")
                   .when((col("value_contribution_cumsum") > 80) & (col("value_contribution_cumsum") <= 95), "B")
                   .otherwise("C"))

df = df.withColumn("qty_contribution_pct_category",
                   when((col("qty_contribution_cumsum") >= 0) & (col("qty_contribution_cumsum") <= 80), "F")
                   .when((col("qty_contribution_cumsum") > 80) & (col("qty_contribution_cumsum") <= 95), "M")
                   .otherwise("S"))

# COMMAND ----------

# DBTITLE 1,Categorise A B C
# Add categorisation column based on combinations
df = df.withColumn("categorisation",
                   when((col("value_contribution_pct_category") == "A") & (col("qty_contribution_pct_category") == "F"), "Runner")
                   .when((col("value_contribution_pct_category") == "A") & (col("qty_contribution_pct_category") == "M"), "Runner")
                   .when((col("value_contribution_pct_category") == "B") & (col("qty_contribution_pct_category") == "F"), "Runner")
                   .when((col("value_contribution_pct_category") == "B") & (col("qty_contribution_pct_category") == "M"), "Repeater")
                   .when((col("value_contribution_pct_category") == "C") & (col("qty_contribution_pct_category") == "F"), "Repeater")
                   .otherwise("Stranger"))

# COMMAND ----------

# DBTITLE 1,Create spark dataframe
df.createOrReplaceTempView("test")

# COMMAND ----------

# DBTITLE 1,NDP value per Part
df_NDP = spark.sql(f"""
    SELECT 
        Part_Id as Part_no,ROUND(value_INR / InvoiceQuantity, 2) AS NDP
    FROM (
        SELECT  MaterialNumber AS Part_Id,
            SUM(InvoiceQuantity) AS InvoiceQuantity,
            SUM(TotalValueinINR) AS value_INR
        FROM oogib_cin.hawkeye.v_ib_parts_flash_dispatch_prod
        WHERE LeoDate BETWEEN (LAST_DAY(ADD_MONTHS(CURRENT_DATE, -5)) + 1) 
                          AND LAST_DAY(ADD_MONTHS(CURRENT_DATE, -2))
        GROUP BY all
    )
""")

# COMMAND ----------

# DBTITLE 1,Convert to spark dataframe
df_NDP.createOrReplaceTempView("test_NDP")

# COMMAND ----------

# DBTITLE 1,Map with NDP value
WH_ABC_FMS = spark.sql("""select a.*,b.NDP from test a left join test_NDP b on a.part_no = b.part_no""")
#WH_ABC_FMS.display()

# COMMAND ----------

# DBTITLE 1,Count check
category_counts = df.groupBy("categorisation").count()
#category_counts.display()

# COMMAND ----------

# DBTITLE 1,Write the data to a table
WH_ABC_FMS.write.mode("overwrite").format("delta").saveAsTable("vision_dev.vision.IB_Spare_3W_ABC_FMS")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vision_dev.vision.IB_Spare_3W_ABC_FMS limit 10

# COMMAND ----------

