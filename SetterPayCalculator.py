import pandas as pd
import os
from datetime import datetime, timedelta
import pyodbc

print('Getting info from Database')
# MSSQL DB Query
connection_string = (
    'connection_string'
)

connection = pyodbc.connect(connection_string)
db_sql_query = """Select 
    Setter, 
    Sales_Office, 
    sq.Date, 
    SUM(Hours) as Hours, 
    SUM(Pitches) as Pitches,
    sum(NP_Missed) as NP_Missed,
    sum(FA_Pitches) as FA_Pitches,
	sum(KW_Sold) as KW_Sold,
	sum(KW_Installed) as KW_Installed

From

(-- this is for the pitches
SELECT
    REPLACE([setter], '(Inactive)', ' ') as Setter,
    [sales_office],
    COALESCE(replace([appointment_date], '00:00:00', ' '), replace(created_date, '00:00:00', ' ')) as Date,
    0 as Hours,
    COUNT(setter) as Pitches,
    0 as NP_Missed,
    0 as FA_Pitches,
	0 as KW_Sold,
	0 as KW_Installed
FROM [podio].[fs_valor_deals] 
WHERE
    COALESCE([appointment_date], created_date) IS NOT NULL
    AND outcome LIKE 'pitched%' 
    AND commission_structure_valor LIKE '%setter%'
    AND setter IS NOT NULL
    AND sales_office IS NOT NULL
GROUP BY COALESCE(replace([appointment_date], '00:00:00', ' '), replace(created_date, '00:00:00', ' ')), [sales_office], [setter]

UNION ALL

-- This is for the hours
SELECT 
    CONCAT([fname], ' ', lname) AS Setter_Name,
    [sales_office],
    replace([local_date], '00:00:00.000', ' ') as DATE,
    SUM(CONVERT(decimal(10, 2), [hours])) AS Hours,
    0 as Pitches,
    0 as NP_Missed,
    0 as FA_Pitches,
	0 as KW_Sold,
	0 as KW_Installed
FROM [dbo].[tsheets_hours_export]
GROUP BY CONCAT([fname], ' ', lname), [sales_office], [local_date]

union all

-- This is for the NP Missed
SELECT
    REPLACE([setter], '(Inactive)', ' ') as Setter,
    [sales_office],
    COALESCE(replace([appointment_date], '00:00:00', ' '), replace(created_date, '00:00:00', ' ')) as Date,
    0 as Hours,
    0 as Pitches,
    count(CASE WHEN outcome LIKE '%Missed%' THEN 1 ELSE 0 END) as NP_Missed,
    0 as FA_Pitches,
	0 as KW_Sold,
	0 as KW_Installed
FROM [podio].[fs_valor_deals] 
WHERE
    COALESCE([appointment_date], created_date) IS NOT NULL
    AND outcome LIKE '%missed%'
    AND commission_structure_valor LIKE '%setter%'
    AND setter IS NOT NULL
    AND sales_office IS NOT NULL
GROUP BY COALESCE(replace([appointment_date], '00:00:00', ' '), replace(created_date, '00:00:00', ' ')), [sales_office], [setter]

union all

-- This is for Pitches that we FA'd
SELECT
    REPLACE([setter], '(Inactive)', ' ') as Setter,
    [sales_office],
    fs.date_company_approved as Date,
    0 as Hours,
    0 as Pitches,
	0 as NP_Missed,
	count(fs.date_company_approved) as FA_Pitches,
	SUM(CAST(pm.system_size AS FLOAT)/1000) as KW_Sold,
	0 as KW_Installed
FROM [podio].[fs_valor_deals] fs
join [podio].[project_management_projects] pm on fs.project_master_id = pm.project_master_id
WHERE
    setter IS NOT NULL
    AND sales_office IS NOT NULL
	and fs.date_company_approved is not null
GROUP BY fs.date_company_approved, [sales_office], [setter]

union all

-- This is for install complete deals 
SELECT
    REPLACE([setter], '(Inactive)', ' ') as Setter,
    [sales_office],
    pmi.install_complete_date as Date,
    0 as Hours,
    0 as Pitches,
	0 as NP_Missed,
	0 as FA_Pitches,
	0 as KW_Sold,
	SUM(CAST(pmi.system_size AS FLOAT)/1000) as KW_Installed
FROM [podio].[fs_valor_deals] fs
join [podio].[project_management_installs] pmi on fs.project_master_id = pmi.project_master_id
WHERE
    setter IS NOT NULL
    AND sales_office IS NOT NULL
	and pmi.date_install_complete is not null
GROUP BY pmi.install_complete_date, [sales_office], [setter]

Union all

-- This is to make sure we have all managers / team leads every week
SELECT
        manager_names.Setter,
        manager_names.Sales_Office,
        DATEADD(DAY, -7, GETDATE()) as Date,  
        0 as Hours,
        0 as Pitches,
        0 as NP_Missed,
        0 as FA_Pitches,
		0 as KW_Sold,
		0 as KW_Installed
    FROM
        (VALUES
            ('Hannah Biggs', 'AB Edmonton'), -- Manager
            ('Zachery Demontigny', 'AB Calgary'), -- Manager
			('Christian Bertsch', 'AB Edmonton'), -- Team Lead
			('Tyler Loxton', 'AB Calgary') -- Team Lead
         ) AS manager_names(Setter, Sales_Office)) sq

where sq.sales_office like '%AB%'
group by Setter, Sales_Office, Date
order by Setter"""

# Execute SQL query
db_setter_info = pd.read_sql(db_sql_query, connection)
connection.close()
# db_setter_info.to_excel('C:/Users/bseljestad/Desktop/Current DB Query Info Setter Pay.xlsx', index=False, )
# temp_db_setter_info = pd.read_excel('C:/Users/bseljestad/Desktop/Current DB Query Info Setter Pay.xlsx', sheet_name='Sheet1')
db_setter_info = db_setter_info.rename(columns={'Sales_Office': 'Sales Office'})
print('Getting payscale info from "NEW Setter Pay Tables Canada.xlsx')

# Canada Setter Pay Tables into pd.DataFrames
excel_wb_filename = 'C:/Users/bseljestad/Desktop/Setting Up Setter Pay Canada/NEW Setter Pay Tables Canada.xlsx'

vet_pay_sheet = 'Veteran Setter Pay'
vet_pay_df = pd.read_excel(excel_wb_filename,sheet_name=vet_pay_sheet)

man_overrides_breakdown_sheet = 'Manager Pay'
man_overrides_breakdown_df = pd.read_excel(excel_wb_filename, sheet_name=man_overrides_breakdown_sheet)

new_setter_pay_sheet = 'New Setter Pay'
new_setter_pay_df = pd.read_excel(excel_wb_filename, sheet_name=new_setter_pay_sheet)

regional_overrides_breakdown_sheet = 'Regional Pay'
regional_overrides_breakdown_df = pd.read_excel(excel_wb_filename, sheet_name=regional_overrides_breakdown_sheet)

team_lead_overrides_sheet = 'Team Lead Pay'
team_lead_overrides_df = pd.read_excel(excel_wb_filename, sheet_name=team_lead_overrides_sheet)
team_lead_overrides_df = team_lead_overrides_df.rename(columns={'Office': 'Sales Office'})
db_setter_info['Date'] = pd.to_datetime(db_setter_info['Date'])
# Adjust to pay date
db_setter_info['Payable Date'] = db_setter_info['Date'] + db_setter_info['Date'].apply(lambda x: timedelta(days=(0 - x.weekday()) % 7)) + timedelta(days=4)

#Pay Calculations Dataframes
weekly_office_stats = db_setter_info[['Sales Office', 'Setter', 'Hours', 'Pitches', 'FA_Pitches', 'NP_Missed', 'KW_Sold', 'KW_Installed','Date', 'Payable Date']]
filtered = weekly_office_stats[(weekly_office_stats['Setter'] == 'Alix Flint')]
# print(filtered)
weekly_office_stats = weekly_office_stats.groupby(['Payable Date', 'Setter', 'Sales Office']).agg({
    'Hours': 'sum',
    'Pitches': 'sum',
    'NP_Missed': 'sum',
    'KW_Sold': 'sum',
    'KW_Installed': 'sum',
    'FA_Pitches': 'sum'
    }).reset_index()
# weekly_office_stats['Pay for Pitches'] = weekly_office_stats['Pitches'] * 30
# weekly_office_stats['Pay for FAs'] = weekly_office_stats['FA_Pitches'] * 350
# weekly_office_stats['Pay for NP_Missed'] = weekly_office_stats['NP_Missed'] * 20
weekly_office_stats = weekly_office_stats.merge(new_setter_pay_df, on='Setter', how='left').merge(\
    team_lead_overrides_df, left_on='Setter', right_on='Team Lead Name', how='left').merge(\
        man_overrides_breakdown_df, left_on='Setter', right_on='Manager Name', how='left')
weekly_office_stats['Pay per Pitch'] = weekly_office_stats['Personal Pitches'] + weekly_office_stats['Per Pitch Rate']
weekly_office_stats['Pitch Pay'] = weekly_office_stats['Pay per Pitch'] * weekly_office_stats['Pitches']
weekly_office_stats['Hour Pay'] = weekly_office_stats['Hours'] * weekly_office_stats['Hourly Rate']
weekly_office_stats['FA Pay'] = weekly_office_stats['FA_Pitches'] * 350
weekly_office_stats['NP Missed Pay'] = weekly_office_stats['NP_Missed'] * 20
# weekly_office_stats['KW FA Sold'] = weekly_office_stats.groupby(['Sales Office', 'Payable Date'])['KW_Sold'].sum().reset_index()
weekly_office_stats = weekly_office_stats[['Payable Date', 'Setter', 'Personal Pitches', 'Pitch Pay', 'Hours', 'Hour Pay', 'FA_Pitches', 'FA Pay', 'NP_Missed', 'NP Missed Pay', 'KW_Sold']]
print(weekly_office_stats[weekly_office_stats['Payable Date'] == pd.to_datetime('2023-10-06')])
# print(weekly_office_stats[weekly_office_stats['Setter'] == 'Alix Flint'].head().to_string(index=False))
# print(weekly_office_stats.sort_values(by='Payable Date', ascending=False).head(60))
# ).reset_index()
# print(weekly_office_stats)
# print(weekly_office_stats.to_string(index=False))
# weekly_office_man_pay = man_overrides_breakdown_df.groupby
# Rename the columns to match the DAX expression
# weekly_office_man_pay.rename(columns={'Sales_Office': 'Sales Office', 'Date': 'Payable Date'}, inplace=True)

# Print the resulting DataFrame
# print(weekly_office_man_pay.to_string(index=False))

# folder = 'C:/Users/bseljestad/Desktop/SetterPayrolls'
# files = os.listdir(folder)
# main_df = pd.DataFrame(columns=['Sales Office', "Sum of Manager Pay, Director Pay, and Kevin's Pay", 'Sum of Setters Pay', 'Divvy', 'Total Pay', 'Date'])
# dates = {}
# for file in files:
#     file_name = os.path.basename(file)
#     file_path = os.path.join(folder, file)
#     if '.xlsx' in file_name and file_name != 'Setter Cost Old Master.xlsx' and file_name != 'Setter Cost Master.xlsx':
#         print(f'Getting info from {file_name}')
#         date_start = file_name.index('.')-2
#         date_end = len(file_name) - 5
#         date = file_name[date_start:date_end].strip()
#         date_format = "%m.%d.%y"
#         parsed_date = datetime.strptime(date, date_format)
#         month = parsed_date.month
#         day = parsed_date.day - 5
#         year = parsed_date.year
#         date = f'{month}/{day}/{year}'
#         dates[datetime.strptime(date, '%m/%d/%Y')] = file_path
#         us_sum = 'USA SUMMARY'
#         can_sum = 'CAN SUMMARY'
#         team = 'TEAM PITCHES'
#         deals = 'DEALS'
#         manager_range = 'A:F'
#         kevin_range = 'H:J'
#         us_df = pd.read_excel(file_path, sheet_name=us_sum)
#         can_df = pd.read_excel(file_path, sheet_name=can_sum)
#         manager_df = pd.read_excel(file_path, sheet_name=team, usecols=manager_range)
#         deals_df = pd.read_excel(file_path, sheet_name=deals)

#         # Calculating Setter Reps (Not Managers) Total Cost
#         for column in can_df.columns:
#             if 'total' in column.lower():
#                 can_total = column
#         for column in us_df.columns:
#             if 'com +' in column.lower():
#                 us_total = column
#         can_setter_total = can_df.groupby('SALES OFFICE')[can_total].sum().reset_index()
#         can_setter_total = can_setter_total.rename(columns={can_total: 'Sum of Setters Pay', 'SALES OFFICE': 'Sales Office'})
#         us_setter_total = us_df.groupby('SALES OFFICE')[us_total].sum().reset_index()
#         us_setter_total = us_setter_total.rename(columns={us_total: 'Sum of Setters Pay', 'SALES OFFICE': 'Sales Office'})
#         setter_pay_totals = pd.concat([us_setter_total, can_setter_total])
#         for row in setter_pay_totals:
#             setter_pay_totals['Date'] = date
#         for column in manager_df.columns:
#             if 'total' in column.lower():
#                 man_total = column
#         manager_office = manager_df.groupby('SALES OFFICE')[man_total].sum().reset_index()
#         deals_total = pd.DataFrame(deals_df.groupby('Sales Office').size().reset_index(name='Count'))
#         manager_office['Total Pay'] = manager_office['Total Pay'] + (deals_total['Count'] * 20)
#         manager_office = manager_office.rename(columns={'SALES OFFICE': 'Sales Office', 'Total Pay': "Sum of Manager Pay, Director Pay, and Kevin's Pay"})
#         for row in manager_office:
#             manager_office['Date'], manager_office['Divvy'] = date, None
#         result = pd.merge(manager_office, setter_pay_totals, how='outer', on=['Sales Office', 'Date'])
#         for row in result:
#             result['Total Pay'] = result['Sum of Setters Pay'] + result["Sum of Manager Pay, Director Pay, and Kevin's Pay"]
#         result = result[['Sales Office', "Sum of Manager Pay, Director Pay, and Kevin's Pay", 'Sum of Setters Pay', 'Divvy', 'Total Pay', 'Date']]
#         main_df = pd.concat([main_df, result])
# for file in files:
#     file_name = os.path.basename(file)
#     file_path = os.path.join(folder, file)
#     if file_name == 'Setter Cost Old Master.xlsx':
#         print(f'Getting info from {file_name}')
#         setter_costs_master = pd.read_excel(file_path, sheet_name='Sheet1')
#         main_df = pd.concat([main_df, setter_costs_master])
    
# # master_file = 'Setter Cost Master.xlsx'
# # master_path = os.path.join(folder, master_file)
# # os.remove(master_path)
# # main_df.to_excel(master_path, index=False, sheet_name='Sheet1')
# # print(f'{master_file} updated.')

# #Sending emails
# current_week_setter_pay = dates[max(dates.keys())]
# deals_data = pd.read_excel(current_week_setter_pay, sheet_name='DEALS')
# print(deals_data.groupby(['Sales Office', 'Setter']).agg('sum').reset_index())