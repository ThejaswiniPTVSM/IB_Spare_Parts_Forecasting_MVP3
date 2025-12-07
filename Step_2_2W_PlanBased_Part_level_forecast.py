# Databricks notebook source
# DBTITLE 1,Import required libraries
import pandas as pd
import numpy as np
import decimal
from datetime import datetime
from dateutil.relativedelta import relativedelta

# COMMAND ----------

# DBTITLE 1,Query and aggregate 2W parts billing data
df = spark.sql(f"""
    SELECT Part_Id, CASE WHEN MONTH(LeoDate) IN (1, 2, 3, 4, 5, 6, 7, 8, 9) THEN CONCAT(YEAR(LeoDate), '-0', MONTH(LeoDate), '-01')
                 ELSE CONCAT(YEAR(LeoDate), '-', MONTH(LeoDate), '-01') END AS start_of_month, 
                 SUM(qty) AS qty, SUM(value_INR) AS value
    FROM (select MaterialNumber as part_id, cast(BillingDate as date) as LeoDate, cast(YF2_InvoiceQuantity as int) qty, YF2_Invoice_ValueinINR as value_INR from oogib_cin.bumblebee.ib_parts_billing_report 
        WHERE BillingDate between (last_day(add_months(current_date, -10)) + 1) AND (last_day(add_months(current_date, -1))) and industry='2W' and Type = 'Parts' 
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

ABC_FMS_data=spark.sql(f""" select distinct Part_no as Part_Id,value_contribution_pct_category as ABC_class,qty_contribution_pct_category as Fsn_class,NDP from vision_dev.vision.IB_Spare_2W_ABC_FMS""").toPandas()
#,'Repeater','Stranger'

# COMMAND ----------

# DBTITLE 1,Read the monthly plan breakdown file
# Read the plan breakdown file once
Plan_BreakDown_full = pd.read_csv('/Workspace/Users/thejaswini.p@tvsmotor.com/IB_Spare_Parts_Forecasting_MVP3/input/2W_3W_Monthly_Plan.csv')

# Prepare base dataframe
df['start_of_month'] = pd.to_datetime(df['start_of_month'])
df['Month'] = df['start_of_month'].dt.to_period('M')

# Calculate historical averages (this remains constant)
grouped_df = df.groupby(['Month', 'Part_Id']).agg({'qty': 'sum'}).reset_index()
monthly_df_base = grouped_df.groupby(['Part_Id']).agg(
    Total_qty=('qty', 'sum'), 
    Months_Count=('Month', 'count')
).reset_index()

monthly_df_base['qty'] = monthly_df_base['Total_qty'] / monthly_df_base['Months_Count']
monthly_df_base['qty'] = monthly_df_base['qty'].astype(float)
monthly_df_base['qty'] = monthly_df_base['qty'].apply(np.ceil)

# Merge with ABC_FMS data once
monthly_df_base = pd.merge(monthly_df_base, ABC_FMS_data, on='Part_Id', how='inner')

# Calculate base values
monthly_df_base['qty'] = monthly_df_base['qty'].astype(float)
monthly_df_base['NDP'] = monthly_df_base['NDP'].apply(decimal.Decimal).astype(float)
monthly_df_base['OrderValue'] = monthly_df_base['qty'] * monthly_df_base['NDP']
monthly_df_base['ABC_FMS'] = monthly_df_base['ABC_class'].str.cat(monthly_df_base['Fsn_class'])

# Define ABC_FMS Ratio conditions
conditions = [
    (monthly_df_base['ABC_FMS'] == 'AF'),
    (monthly_df_base['ABC_FMS'] == 'AM'),
    (monthly_df_base['ABC_FMS'] == 'BF'),
    (monthly_df_base['ABC_FMS'] == 'BM'),
    (monthly_df_base['ABC_FMS'] == 'CF'),
    (monthly_df_base['ABC_FMS'] == 'AS'),
    (monthly_df_base['ABC_FMS'] == 'BS'),
    (monthly_df_base['ABC_FMS'] == 'CM'),
    (monthly_df_base['ABC_FMS'] == 'CS')
]
choices = [4, 4, 2, 2, 1, 1, 1, 1, 1]
monthly_df_base['ABC_FMS_Ratio'] = np.select(conditions, choices, default=0)

# Calculate total order value for base
monthly_df_base['Total_OrderValue'] = monthly_df_base['OrderValue'].sum()

# Initialize list to store results
all_forecasts = []

# Get current date and calculate next month
current_date = datetime.now()
next_month = current_date + relativedelta(months=1)

# Loop through 12 months
for i in range(12):
    # Calculate the target month
    target_date = next_month + relativedelta(months=i)
    monthname = target_date.strftime('%b-%y')
    
    print(f"Processing month: {monthname}")
    
    # Create a copy of base dataframe for this month
    monthly_df = monthly_df_base.copy()
    
    # Filter plan for current month and 2W type
    Plan_BreakDown = Plan_BreakDown_full[
        (Plan_BreakDown_full['Month'] == monthname) & 
        (Plan_BreakDown_full['Type'] == '2W')
    ][['Plan']]
    
    # Get plan value (use 0 if no plan exists for this month)
    if len(Plan_BreakDown) > 0:
        plan_value = Plan_BreakDown['Plan'].sum()
    else:
        print(f"Warning: No plan found for {monthname}, using 0")
        plan_value = 0
    
    monthly_df['Plan'] = plan_value
    
    # Compute weight factors
    monthly_df['Weighted_Ratio'] = ((monthly_df['ABC_FMS_Ratio'] * 0.7) +(monthly_df['qty'] / monthly_df['Total_qty'].sum() * 0.3))
    
    # Adjust plan_and_order
    monthly_df['plan_and_order_adjusted'] = ((monthly_df['Plan'] - monthly_df['Total_OrderValue']) / monthly_df['Plan']) * (monthly_df['Weighted_Ratio']) if plan_value != 0 else 0
    
    # Compute Updated_Qty
    monthly_df['Updated_Qty'] = (monthly_df['qty'] * (1 + monthly_df['plan_and_order_adjusted']))
    
    # Round and calculate final values
    monthly_df['Rounded_Qty'] = monthly_df['Updated_Qty'].apply(np.ceil)
    monthly_df['upadte_qty_ndp'] = monthly_df['Rounded_Qty'] * monthly_df['NDP']
    
    # Convert to float and ensure non-negative
    monthly_df['Updated_Qty'] = monthly_df['Updated_Qty'].astype(float).apply(lambda x: max(0, x))
    monthly_df['Rounded_Qty'] = monthly_df['Rounded_Qty'].astype(float).apply(lambda x: max(0, x))
    monthly_df['upadte_qty_ndp'] = monthly_df['upadte_qty_ndp'].astype(float).apply(lambda x: max(0, x))
    
    # Add forecast month
    monthly_df['Forecast_month'] = monthname
    
    print(f"{monthname} - Total forecast value: {monthly_df['upadte_qty_ndp'].sum():,.2f}")
    
    # Select final columns
    monthly_forecast = monthly_df[['Part_Id', 'Forecast_month', 'Rounded_Qty', 'ABC_FMS']].copy()
    monthly_forecast['Forecast_month'] = pd.to_datetime(monthly_forecast['Forecast_month'], format='%b-%y') + pd.offsets.MonthBegin(0)
    monthly_forecast['industry'] = '2W'
    
    # Append to results
    all_forecasts.append(monthly_forecast)

# Combine all forecasts into one dataframe
final_forecast_df = pd.concat(all_forecasts, ignore_index=True)

print(f"\nTotal rows in final forecast: {len(final_forecast_df)}")
print(f"Months covered: {final_forecast_df['Forecast_month'].nunique()}")
print("\nForecast summary by month:")
print(final_forecast_df.groupby('Forecast_month')['Rounded_Qty'].sum())

# Optionally save to CSV
# final_forecast_df.to_csv('12_month_forecast.csv', index=False)

# COMMAND ----------

# DBTITLE 1,Convert the final forecast DataFrame
spark_df = spark.createDataFrame(final_forecast_df)
spark_df.write.mode("overwrite").format("delta").partitionBy("Forecast_month").saveAsTable("vision_dev.vision.monthly_plan_allocation_2W_base")