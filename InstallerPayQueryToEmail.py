import pyodbc, os
import pandas as pd
import win32com.client as win32

'''
This script will run a sql query in the Azure database and get pay info for all installers based on install date and inspection date.
It will then query the Azure database and get the email addresses for the installers in Podio and send an email to each installer with a 
breakdown csv attached with their pay info for each job and a pay summary table in the body of the email.  It will also send a master list to 
cbuston@companysolar.com with all installer pay info.

When you run this script change the payable_date to the date pay will be disbursed and change start_date and end_date to the date range
installers work will be paid out on the payable_date.
'''
payable_date = '2023-11-10'
pay_breakdown_file = 'MyPayBreakdown.csv'
def get_sql_info():
    '''
    This func will get all installer work and pay info for ranges profided and return it as a pd.Dataframe() (pay_df).
    It then gets all emails for installers and returns it as a pd.Dataframe (email_df)
    It then returns an overview of pay_df grouped by installer as a pd.DataFrame() (overview_pay_df)
    '''
    start_date = '2023-10-23'
    end_date = '2023-11-06'

    # Define the connection string
    connection_string = (
        "connection_string"
    )

    # Establish the database connection
    connection = pyodbc.connect(connection_string)
    pay_query = f'''
    DECLARE @StartDate DATE,
            @EndDate DATE;
    SET @StartDate = '{start_date}';
    SET @EndDate = '{end_date}';
    WITH idvQuery AS (SELECT
    fip.customer_name_installer_pay AS 'Customer',
    pmp.region AS 'Region',
    fip.adders_added AS 'Adders',
    fip.project_master_id_installer_pay AS 'Master ID',
    fip.installers_name_installer_pay AS 'Installer',
    CAST(fip.installer_pay_rate_installer_pay AS FLOAT) AS 'Idv Pay per Watt',
    CAST(fip.system_size_from_design_installer_pay AS INT) AS 'System Size',
    fip.date_install_complete_installer_pay AS 'Install Complete Date',
    fip.date_inspection_complete_installer_pay AS 'Inspection Complete Date',
    ROW_NUMBER() OVER (PARTITION BY pmp.customer_name ORDER BY pmp.customer_name) AS RowNum
    FROM
    podio.field_operations_companyinstallerpay fip
    LEFT JOIN
    podio.project_management_projects pmp
    ON fip.project_master_id_installer_pay = pmp.project_master_id),

    aggQuery AS (SELECT
    idv.[Master ID] AS 'Master ID',
    idv.Customer AS 'Customer',
    MAX(idv.Region) AS 'Region',
    idv.Adders AS 'Adders',
    MAX(RowNum) AS '# of Installers',
    MAX(idv.[System Size]) AS 'System Size',
    CASE WHEN MAX(idv.Region) IN ('KS', 'PA', 'TX', 'UT')
        THEN .2 ELSE 0.22 END AS 'Install Pay per Watt per Project',
    MAX(CASE WHEN idv.Adders IS NOT NULL
        THEN LEN(idv.Adders) - LEN(REPLACE(idv.Adders, ',', '')) + 1 
        ELSE 0 END) AS '# of Adders',
    CASE WHEN idv.Adders LIKE '%Ground Mount%'
        THEN .2 / MAX(RowNum) ELSE 0 END AS 'Ground Mount Cost',
    CASE WHEN idv.Adders LIKE '%Steep Roof%'
        THEN .01 ELSE 0 END AS 'Steep Roof Cost',
    CASE WHEN idv.Adders LIKE '%Multi Array%'
        THEN .01 ELSE 0 END AS 'Multi Array (4+) Cost',
    -- CASE WHEN design.trenching_concrete_ground_footage + design.trenching_raw_ground_footage > 0
    --     THEN (6 / MAX(RowNum)) * (design.trenching_raw_ground_footage + design.trenching_concrete_ground_footage)
    --     ELSE 
        0 AS 'Trenching Cost',
    SUM(CAST(idv.[Idv Pay per Watt] AS FLOAT)) AS 'Installer Pay Rate',
    CASE WHEN MAX(idv.Region) IN ('PA', 'TX', 'UT')
        THEN .2 ELSE 0.22 END - SUM(CAST(idv.[Idv Pay per Watt] AS FLOAT)) AS 'Pay per project - Installer Pay',
    MAX(idv.[System Size]) AS 'Watts',
    MAX(idv.[Install Complete Date]) AS 'Install Complete Date',
    MAX(idv.[Inspection Complete Date]) AS 'Inspection Complete Date'
    FROM
    idvQuery idv
    -- LEFT JOIN
    -- podio.project_management_designs desgin
    -- ON design.project_master_id = idv.[Master ID]
    GROUP BY
    idv.[Master ID], idv.Customer, idv.Adders),

    maindataquery AS (
    SELECT
    idv.Customer,
    idv.Installer,
    idv.[Idv Pay per Watt],
    agg.Region,
    agg.Adders,
    agg.[# of Installers],
    agg.[System Size],
    agg.[Installer Pay Rate] AS 'Total Pay Rate',
    agg.[Install Pay per Watt per Project] AS 'Per Region Pay Rate',
    CAST(idv.[Install Complete Date] AS DATE) AS 'Install Complete Date',
    CAST(idv.[Inspection Complete Date] AS DATE) AS 'Inspection Complete Date',
    agg.[Pay per project - Installer Pay] AS 'Pay per Region - Total Installer Pay',
    agg.[Install Pay per Watt per Project],
    idv.[Idv Pay per Watt] AS 'Pay per Watt',
    agg.[Ground Mount Cost] AS 'Ground Mount Cost',
    agg.[Steep Roof Cost] AS 'Steep Roof Cost',
    agg.[Multi Array (4+) Cost] AS 'Multi Array Cost',
    agg.[Trenching Cost] AS 'Trenching Cost',
    CASE WHEN agg.[Pay per project - Installer Pay] <= 0 
        THEN idv.[Idv Pay per Watt] + agg.[Ground Mount Cost] + agg.[Steep Roof Cost] + agg.[Multi Array (4+) Cost]
    ELSE (agg.[Pay per project - Installer Pay] / agg.[# of Installers])
        + idv.[Idv Pay per Watt] + agg.[Ground Mount Cost] + agg.[Steep Roof Cost] + agg.[Multi Array (4+) Cost]
        END AS 'Adjusted Pay Per Watt',
        -- ***** DOES NOT FACTOR TRENCHING COST CORRECTLY BUT ALSO NOT IN USE
    -- ROUND(CASE WHEN agg.[Pay per project - Installer Pay] <= 0 
    --     THEN idv.[Idv Pay per Watt] + agg.[Ground Mount Cost] + agg.[Steep Roof Cost] + agg.[Multi Array (4+) Cost]
    --     + agg.[Trenching Cost]
    -- ELSE (agg.[Pay per project - Installer Pay] / agg.[# of Installers])
    --     + idv.[Idv Pay per Watt] + agg.[Ground Mount Cost] + agg.[Steep Roof Cost] + agg.[Multi Array (4+) Cost]
    --     END * agg.[Watts], 2) AS 'Total Pay',
    CASE WHEN agg.Region IN ('Canada', 'TX', 'UT')
        THEN ROUND(
            CAST(CASE WHEN agg.[Pay per project - Installer Pay] <= 0 
                THEN ((idv.[Idv Pay per Watt] + agg.[Ground Mount Cost] + agg.[Steep Roof Cost] + agg.[Multi Array (4+) Cost]) * agg.[System Size])
                    + agg.[Trenching Cost]
                ELSE (((agg.[Pay per project - Installer Pay] / agg.[# of Installers])
                    + idv.[Idv Pay per Watt] + agg.[Ground Mount Cost] + agg.[Steep Roof Cost] + agg.[Multi Array (4+) Cost])
                * agg.[Watts]) + agg.[Trenching Cost] END AS FLOAT), 2)
        ELSE
            CASE WHEN agg.[Pay per project - Installer Pay] <= 0 
                THEN CAST((((idv.[Idv Pay per Watt] + agg.[Ground Mount Cost] + agg.[Steep Roof Cost] + agg.[Multi Array (4+) Cost])
                    * agg.[System Size]) + agg.[Trenching Cost]) * .8 AS FLOAT)
                ELSE CAST(((((agg.[Pay per project - Installer Pay] / agg.[# of Installers])
                    + idv.[Idv Pay per Watt] + agg.[Ground Mount Cost] + agg.[Steep Roof Cost] + agg.[Multi Array (4+) Cost])
                * agg.[Watts]) + agg.[Trenching Cost]) * .8 AS FLOAT) END END AS 'Install Complete Pay',
    CASE WHEN agg.Region IN ('Canada', 'TX', 'UT')
        THEN 0.00
        ELSE
            CASE WHEN agg.[Pay per project - Installer Pay] <= 0 
                THEN CAST((((idv.[Idv Pay per Watt] + agg.[Ground Mount Cost] + agg.[Steep Roof Cost] + agg.[Multi Array (4+) Cost])
                    * agg.[System Size]) + agg.[Trenching Cost]) * .8 AS FLOAT)
                ELSE CAST(((((agg.[Pay per project - Installer Pay] / agg.[# of Installers])
                    + idv.[Idv Pay per Watt] + agg.[Ground Mount Cost] + agg.[Steep Roof Cost] + agg.[Multi Array (4+) Cost])
                * agg.[Watts]) + agg.[Trenching Cost]) * .2 AS FLOAT) END END AS 'Inspection Complete Pay',
    CASE WHEN agg.[Pay per project - Installer Pay] < 0 THEN CONCAT('Over Budget (',ROUND(0 - agg.[Pay per project - Installer Pay], 2), ' cpW)')
    ELSE 'Within Budget' END AS 'Budget Status'
    FROM idvQuery idv
    JOIN aggQuery agg
    ON idv.[Master ID] = agg.[Master ID])

    SELECT
    main.Customer,
    main.[Region],
    main.Installer,
    main.Adders,
    CASE WHEN main.[Install Complete Date] > '2000-01-01'
        THEN main.[Install Complete Date] ELSE NULL END AS 'Install Complete Date',
    CASE WHEN main.[Inspection Complete Date] > '2000-01-01'
        THEN main.[Inspection Complete Date] ELSE NULL END AS [Inspection Complete Date],
    main.[Install Pay per Watt per Project] AS 'Region Total Pay per Watt',
    main.[Idv Pay per Watt] AS 'Base Pay per Watt',
    main.[Ground Mount Cost] AS 'Grount Mount Pay',
    main.[Trenching Cost] AS 'Trenching Pay',
    main.[System Size],
    CASE WHEN main.[Install Complete Date] BETWEEN @StartDate and @EndDate
        THEN ROUND(main.[Install Complete Pay], 2)
        ELSE 0 END AS 'Install Complete Pay',
    CASE WHEN main.[Inspection Complete Date] BETWEEN @StartDate and @EndDate
        THEN ROUND(main.[Inspection Complete Pay], 2)
        ELSE 0 END AS 'Inspection Complete Pay',
    ROUND(CASE WHEN main.[Install Complete Date] BETWEEN @StartDate and @EndDate
        THEN main.[Install Complete Pay]
        ELSE 0 END +
    CASE WHEN main.[Inspection Complete Date] BETWEEN @StartDate and @EndDate
        THEN main.[Inspection Complete Pay]
        ELSE 0 END, 2) AS 'Total Pay',
    main.[Budget Status]
    FROM maindataquery main
    WHERE main.[Install Complete Date] BETWEEN @StartDate AND @EndDate
    OR main.[Inspection Complete Date] BETWEEN @StartDate AND @EndDate
    ORDER BY main.[Installer] ASC
    '''
    # Execute SQL query
    pay_df = pd.read_sql(pay_query, connection)

    connection.close()


    # Get Emails for isntallers here
    connection = pyodbc.connect(connection_string)
    email_query = '''
    SELECT
    NULL AS 'Eventually an email query will go here.  This needs to come from podio.hr_employees'
    '''
    email_df = pd.read_sql(email_query, connection)

    connection.close()


    # Getting an overview of the pay_df for email format here
    overview_pay_df = pay_df.groupby('Installer').agg({
        'Total Pay': 'sum',
        'System Size': 'sum'
    }).reset_index()
    overview_pay_df = overview_pay_df.rename(columns={'Total Pay': 'Total Pay', 'System Size': 'Total Watts Installed'})
    overview_pay_df['Pay Period'] = f'Total Pay for {start_date} through {end_date}'
    return pay_df, overview_pay_df, email_df

def send_email(distribution, email_address, subject, my_table, my_attachment, cc=[]):
    '''
    This will check if there is a distribution or a cc and then send the email to the email_address with my_table in the body of the email and
    my_attachment as an attachment to the email.
    '''
    outlook = win32.Dispatch('outlook.application')
    mail = outlook.CreateItem(0)
    if distribution != 0:
        mail.SentOnBehalfOfName = distribution
    if cc != 0:
        for rec in cc:
            mail.CC = mail.CC + ';' + rec
    mail.To = email_address
    mail.Subject = subject
    table_html = my_table.to_html(index=False).replace('border="1"','border="0"')
    # open_message = "<br><h4>If you need to report a discrepancy please respond to let us know.</h4>"
    open_message = 'This is a test email for automating the installer pay calculations.  This does not factor in pay for trenching.  This is between 10/23/2023 and 11/06/2023.'
    entire_message = "{} {}".format(table_html,open_message)
    mail.HTMLBody = entire_message
    mail.Attachments.Add(my_attachment)
    mail.Send()
    print("Success! Email Sent to {}".format(email_address))
    return

pay_df, overview_pay_df, email_df = get_sql_info()
count = 0

# Send all pay info to cody buxton
pay_df = pay_df[['Customer', 'Region', 'Installer', 'Adders', 'Install Complete Date', 'Inspection Coplete Date', 'Base Pay per Watt', 'Install Complete Pay', 'Inspection Complete Pay', 'Total Pay', 'Budget Status']]
pay_df.to_csv(pay_breakdown_file, index=False)
my_attachment = os.path.join(os.getcwd(), pay_breakdown_file)
send_email(0, 'cbuxton@company.com', '', overview_pay_df, my_attachment, cc=['bseljestad@company.com','dregehr@company.com'])

# # getting individual excel file pd.df for email content and calling send_email
# for installer in overview_pay_df['Installer']:
#     # 
#     # 
#     # 
#     # Need to remove count condition when ready to run script to send emails to installers
#     # 
#     # 
#     # 
#     if count == 1:
#         count = 0
#         distribution = 0
#         idv_pay_df = pay_df[pay_df['Installer'] == installer][['Customer', 'Region', 'Installer', 'Install Complete Pay', 'Inspection Complete Pay', 'Total Pay']] # Pay attachment
#         idv_overview_df = overview_pay_df[overview_pay_df['Installer'] == installer] # Email table content
#         # installer_email = email_df[email_df['employee'].lower().strip(' ') == installer.lower().strip(' ')] # Email address for employee
#         idv_pay_df.to_csv(pay_breakdown_file, index=False)
#         my_attachment = os.path.join(os.getcwd(), pay_breakdown_file)
#         subject = 'Installer Pay for {} {}'.format(installer, payable_date)
