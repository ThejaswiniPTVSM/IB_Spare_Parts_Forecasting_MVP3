# Databricks notebook source
import pandas as pd
import numpy as np
import decimal
from datetime import datetime

# COMMAND ----------

df = spark.sql(f"""
    SELECT Part_Id, CASE WHEN MONTH(LeoDate) IN (1, 2, 3, 4, 5, 6, 7, 8, 9) THEN CONCAT(YEAR(LeoDate), '-0', MONTH(LeoDate), '-01')
                 ELSE CONCAT(YEAR(LeoDate), '-', MONTH(LeoDate), '-01') END AS start_of_month, 
                 SUM(qty) AS qty, SUM(value_INR) AS value
    FROM (select MaterialNumber as part_id, cast(BillingDate as date) as LeoDate, cast(YF2_InvoiceQuantity as int) qty, YF2_Invoice_ValueinINR as value_INR from bumblebee.ib_parts_billing_report 
        WHERE BillingDate between (last_day(add_months(current_date, -10)) + 1) AND (last_day(add_months(current_date, -2))) and industry='2W' and Type = 'Parts' 
        GROUP BY all) group by all
""").toPandas()

#Total Part_Id count
print(df['Part_Id'].nunique())

#Selecting active parts
Active_Parts=pd.read_csv('/Workspace/Users/thejaswini.p@tvsmotor.com/IB_Spare_Parts_MVP1/Input/Active_Parts_Master.csv', encoding='ISO-8859-1')
Active_Parts.rename(columns={'2W_Part_Id': 'Part_Id','Alternate_Part_Id':'Alternate_Part_No'}, inplace=True)
Active_Parts = Active_Parts[Active_Parts['Part_Id'].notnull()]

#No Battery parts
Active_Parts = Active_Parts[~Active_Parts['Part_Description'].str.contains('BATTERY', case=False, na=False)]
Active_Parts = Active_Parts[['Part_Id']]

#Selecting only active parts and No Battery Parts
df = pd.merge(df, Active_Parts, on='Part_Id', how='inner')

ABC_FMS_data=spark.sql(f""" select distinct Part_no as Part_Id,value_contribution_pct_category as ABC_class,qty_contribution_pct_category as Fsn_class,NDP from vision.IB_Spare_2W_ABC_FMS""").toPandas()
#,'Repeater','Stranger'

# COMMAND ----------

df['start_of_month'].max()

# COMMAND ----------

Plan_BreakDown=pd.read_csv('/Workspace/Users/thejaswini.p@tvsmotor.com/IB_Spare_Parts_Forecasting_MVP3/input/2W_3W_Monthly_Plan.csv')
monthname='Nov-24'
#Dec-24, Jan-25, Feb-25, Mar-25, Apr-25,May-25,Jun-25,Jul-25,Aug-25,Sep-25

Plan_BreakDown = Plan_BreakDown[Plan_BreakDown['Month'] == monthname]
Plan_BreakDown = Plan_BreakDown[Plan_BreakDown['Type'] == '2W']
Plan_BreakDown=Plan_BreakDown[['Plan']]
print(Plan_BreakDown['Plan'].sum())

df['start_of_month'] = pd.to_datetime(df['start_of_month'])
df['Month']=df['start_of_month'].dt.to_period('M')
grouped_df = df.groupby(['Month','Part_Id']).agg({'qty': 'sum'}).reset_index()
monthly_df = grouped_df.groupby(['Part_Id']).agg(Total_qty=('qty', 'sum'), Months_Count=('Month', 'count')).reset_index()
monthly_df['qty'] = monthly_df['Total_qty'] / monthly_df['Months_Count']
monthly_df['qty'] = monthly_df['qty'].astype(float)
monthly_df['qty'] = monthly_df['qty'].apply(np.ceil)

monthly_df = pd.merge(monthly_df, ABC_FMS_data, on='Part_Id', how='inner')

monthly_df['qty'] = monthly_df['qty'].astype(float)
monthly_df['NDP'] = monthly_df['NDP'].apply(decimal.Decimal).astype(float)
monthly_df['OrderValue'] = monthly_df['qty'] * monthly_df['NDP']
monthly_df['ABC_FMS'] = monthly_df['ABC_class'].str.cat(monthly_df['Fsn_class'])

conditions = [
    (monthly_df['ABC_FMS'] == 'AF'),
    (monthly_df['ABC_FMS'] == 'AM'),
    (monthly_df['ABC_FMS'] == 'BF'),
    (monthly_df['ABC_FMS'] == 'BM'),
    (monthly_df['ABC_FMS'] == 'CF'),
    (monthly_df['ABC_FMS'] == 'AS'),
    (monthly_df['ABC_FMS'] == 'BS'),
    (monthly_df['ABC_FMS'] == 'CM'),
    (monthly_df['ABC_FMS'] == 'CS')
]

# Define the choices
choices = [20,20,10,5,3,2,1,1,1]
# Apply np.select to create the 'ABC_FMS_Ratio' column
monthly_df['ABC_FMS_Ratio'] = np.select(conditions, choices, default=0)

#plan = np.full(len(monthly_df), Plan_BreakDown['Plan'].values[0])
#monthly_df['Plan'] = plan

monthly_df['Plan']=479824402.6

monthly_df['Total_OrderValue'] = monthly_df['OrderValue'].sum()

# Compute weight factors based on historical performance and part importance
monthly_df['Weighted_Ratio'] = (
    (monthly_df['ABC_FMS_Ratio'] * 0.7) +  # More weight to ABC_FMS Ratio
    (monthly_df['qty'] / monthly_df['Total_qty'].sum() * 0.3)  # Weight to part's relative demand
)

# Adjust 'plan_and_order' to incorporate deviations
monthly_df['plan_and_order_adjusted'] = (
    (monthly_df['Plan'] - monthly_df['Total_OrderValue']) / monthly_df['Plan']
) * (monthly_df['Weighted_Ratio'])

# Compute Updated_Qty with non-linear scaling
monthly_df['Updated_Qty'] = (
    monthly_df['qty'] * (1 + monthly_df['plan_and_order_adjusted'])
)

# monthly_df['plan_and_order'] = (monthly_df['Plan'] - monthly_df['Total_OrderValue']) / monthly_df['Plan']
# monthly_df['Updated_Qty'] = monthly_df['qty'] + (monthly_df['plan_and_order'] * monthly_df['ABC_FMS_Ratio'] * monthly_df['qty'])

monthly_df['Rounded_Qty'] = monthly_df['Updated_Qty'].apply(np.ceil)
monthly_df['upadte_qty_ndp'] = monthly_df['Rounded_Qty'] * monthly_df['NDP']
monthly_df['Updated_Qty'] = monthly_df['Updated_Qty'].astype(float)
monthly_df['Rounded_Qty'] = monthly_df['Rounded_Qty'].astype(float)
monthly_df['upadte_qty_ndp'] = monthly_df['upadte_qty_ndp'].astype(float)
monthly_df['Updated_Qty'] = monthly_df['Updated_Qty'].apply(lambda x: max(0, x))
monthly_df['Rounded_Qty'] = monthly_df['Rounded_Qty'].apply(lambda x: max(0, x))
monthly_df['upadte_qty_ndp'] = monthly_df['upadte_qty_ndp'].apply(lambda x: max(0, x))
monthly_df['Forecast_month'] = monthname

print(monthly_df['upadte_qty_ndp'].sum())


# COMMAND ----------

df = spark.sql(f"""
    SELECT Part_Id, CASE WHEN MONTH(LeoDate) IN (1, 2, 3, 4, 5, 6, 7, 8, 9) THEN CONCAT(YEAR(LeoDate), '-0', MONTH(LeoDate), '-01')
                 ELSE CONCAT(YEAR(LeoDate), '-', MONTH(LeoDate), '-01') END AS start_of_month, 
                 SUM(qty) AS qty, SUM(value_INR) AS value
    FROM (select MaterialNumber as part_id, cast(BillingDate as date) as LeoDate, cast(YF2_InvoiceQuantity as int) qty, YF2_Invoice_ValueinINR as value_INR from bumblebee.ib_parts_billing_report 
        WHERE BillingDate between (last_day(add_months(current_date, -2)) + 1) AND (last_day(add_months(current_date, -1))) and industry='2W' and Type = 'Parts' 
        GROUP BY all) group by all
""").toPandas()

#Total Part_Id count
print(df['Part_Id'].nunique())

#Selecting active parts
Active_Parts=pd.read_csv('/Workspace/Users/thejaswini.p@tvsmotor.com/IB_Spare_Parts_MVP1/Input/Active_Parts_Master.csv', encoding='ISO-8859-1')
Active_Parts.rename(columns={'2W_Part_Id': 'Part_Id','Alternate_Part_Id':'Alternate_Part_No'}, inplace=True)
Active_Parts = Active_Parts[Active_Parts['Part_Id'].notnull()]

#No Battery parts
Active_Parts = Active_Parts[~Active_Parts['Part_Description'].str.contains('BATTERY', case=False, na=False)]
Active_Parts = Active_Parts[['Part_Id']]

#Selecting only active parts and No Battery Parts
df_actual = pd.merge(df, Active_Parts, on='Part_Id', how='inner')

# COMMAND ----------

# Merge actual data with forecast data on 'part' and 'month'
df_actual_forecast = pd.merge(
    df_actual,  # 'qty' represents actual demand
    monthly_df[['Part_Id','Forecast_month','Rounded_Qty']],
    on=['Part_Id'],
    how='inner'
)

# COMMAND ----------

df_actual_forecast.head(5)

# COMMAND ----------

df_actual_forecast['Absolute_error'] = ((df_actual_forecast['qty'] - df_actual_forecast['Rounded_Qty']).abs())

# COMMAND ----------

df_actual_forecast['Absolute_error'].sum()

# COMMAND ----------

df_actual_forecast['qty'].sum()

# COMMAND ----------

df_actual_forecast['Rounded_Qty'].sum()
#2242361.0, 2250442.0

# COMMAND ----------

1-(df_actual_forecast['Absolute_error'].sum()/df_actual_forecast['qty'].sum())

# COMMAND ----------

df_actual_forecast['Part_Id'].nunique()

# COMMAND ----------

monthly_df=monthly_df[['Part_Id','Forecast_month','Rounded_Qty']]