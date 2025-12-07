# Databricks notebook source
import pandas as pd

# COMMAND ----------

IB_Spares=spark.sql(f"""select order_date, part_id as Part_Id,Country, sum(order_qty) as forecast_qty from vision.IB_industry_country_spare where run_date='2025-04-03' and order_date='2025-05-01' group by order_date, part_id,Country """).toPandas()

# COMMAND ----------

IB_Spares.display()

# COMMAND ----------

df_2W = spark.sql(f"""
    SELECT Part_Id, CASE WHEN MONTH(LeoDate) IN (1, 2, 3, 4, 5, 6, 7, 8, 9) THEN CONCAT(YEAR(LeoDate), '-0', MONTH(LeoDate), '-01')
                 ELSE CONCAT(YEAR(LeoDate), '-', MONTH(LeoDate), '-01') END AS start_of_month, 
                 SUM(InvoiceQuantity) AS qty, SUM(value_INR) AS value
    FROM (SELECT Country, MaterialNumber AS part_id, DealerCode,LeoDate, sum(InvoiceQuantity) as InvoiceQuantity,SUM(TotalValueinINR) AS value_INR
        FROM hawkeye.v_ib_parts_flash_dispatch_prod
        WHERE LeoDate between (last_day(add_months(current_date, -2)) + 1) AND (last_day(add_months(current_date, -1)))
        GROUP BY all
    ) AS a
    INNER JOIN (
        SELECT DISTINCT MaterialNumber, industry
        FROM bumblebee.ib_parts_billing_report
        WHERE industry='2W' and Type = 'Parts' 
    ) AS b
    ON a.part_id = b.MaterialNumber group by all
""").toPandas()

# COMMAND ----------

df_2W.display()

# COMMAND ----------

df_NDP = spark.sql(f"""
    SELECT 
        Part_Id,
        ROUND(value_INR / InvoiceQuantity, 2) AS NDP
    FROM (
        SELECT  DealerCode,
            MaterialNumber AS Part_Id,
            SUM(InvoiceQuantity) AS InvoiceQuantity,
            SUM(TotalValueinINR) AS value_INR
        FROM hawkeye.v_ib_parts_flash_dispatch_prod
        WHERE LeoDate BETWEEN (LAST_DAY(ADD_MONTHS(CURRENT_DATE, -5)) + 1) 
                          AND LAST_DAY(ADD_MONTHS(CURRENT_DATE, -2))
        GROUP BY all
    )
""").toPandas()

# COMMAND ----------

df_NDP.display()

# COMMAND ----------

