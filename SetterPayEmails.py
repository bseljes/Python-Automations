import pyodbc, os
import win32com.client as win32
from datetime import datetime
import pandas as pd


def get_current_paydate_file():
    # Get MAX(date) from files in folder for setter payrolls
    folder = 'C:/Users/bseljestad/Desktop/SetterPayrolls'
    dates = {}
    for file in os.listdir(folder):
        if file[-5:] == '.xlsx':
            date = file.strip().split(' ')[-1][:-5].replace('.',' ')
            date = date.strip().split()
            date[0], date[1], date[2] = date[2], date[0], date[1]
            date = '-'.join(date)
            date = datetime.strptime(date, "%Y-%m-%d").date()
            dates[date] = os.path.join(folder, file)
    curr_paydate_file = dates[max(dates.keys())]

    def get_dfs_from_file(curr_paydate_file):
        # Getting all pay data into pd.df
        manager_df = pd.read_excel(curr_paydate_file, sheet_name='Managers')
        team_lead_df = pd.read_excel(curr_paydate_file, sheet_name='Team Lead')
        setter_df = pd.read_excel(curr_paydate_file, sheet_name='Setters')
        regional_df = pd.read_excel(curr_paydate_file, sheet_name='Regional')
        return manager_df, team_lead_df, setter_df, regional_df
    
    return get_dfs_from_file(curr_paydate_file)

def set_dfs(manager_df, team_lead_df, setter_df, regional_df):
    # Getting total manager pay by joining manager with setter info
    manager_df = pd.merge(left=manager_df, right=setter_df, left_on='Manager Name', right_on='Setter', how='left')[['Manager Name', 'Sales_Office', 'Payable_Date', 'Base Pay', 'Manager Override Pay',
                                                                                                                    'Total Manager Pay', 'Pitches', 'Hours', 'NP Missed Pay', 'FA Pitches Pay', 'Hourly Pay', 'Total Personal Pay']]
    manager_df['Total Pay'] = manager_df['Total Personal Pay'] + manager_df['Total Manager Pay']


    # Getting total team lead pay by joining team lead with team lead info
    team_lead_df = pd.merge(left=team_lead_df, right=setter_df, left_on='Team Lead Name', right_on='Setter', how='left')[['Team Lead Name', 'Sales_Office', 'Payable_Date', 'Team Lead Pay', 'Pitches',
                                                                                                                          'Hours', 'NP Missed Pay', 'FA Pitches Pay', 'Pitch Pay', 'Hourly Pay',
                                                                                                                          'Total Personal Pay']]
    team_lead_df['Total Pay'] = team_lead_df['Team Lead Pay'] + team_lead_df['Total Personal Pay']


    # Need to remove the managers and team leads from the setters_df as all their info is in other dfs.
    manager_names = manager_df['Manager Name'].tolist()
    team_lead_names = team_lead_df['Team Lead Name'].tolist()
    # Grouping region pay by Regional manager name
    agg_functions = {
    'Payable_Date': 'first',  # Keep the first 'Payable_Date' value
    'Regional Pay': 'sum'     # Sum the 'Regional Pay' values
    }

    regional_df = regional_df.groupby(['Regional Name']).agg(agg_functions).reset_index()


    # Use boolean indexing to remove rows from setter_df where 'Setter' is in manager_names or team_lead_names
    setter_df = setter_df[~setter_df['Setter'].isin(manager_names + team_lead_names)]
    return manager_df, team_lead_df, setter_df, regional_df

def get_emails_df():
    # Define the connection string
    connection_string = (
        'connection_string'
    )

    # Establish the database connection
    connection = pyodbc.connect(connection_string)

    # Sql query to get emails
    sqldf = pd.read_sql("""SELECT
    setter_name,
    REPLACE(REPLACE(company_email, 'other:', ''), 'work:', '') AS 'email'
    FROM
    podio.setups_setters""", connection)

    connection.close()

    return sqldf

def send_emails(manager_df, team_lead_df, setter_df, regional_df, emails_df):
    def iter_df_send_email(df, name_col, payable_date):
        for name in df[name_col]:
            email = emails_df[emails_df['setter_name'] == name]['email'].values[0]
            individual_pay_info = df.loc[df[name_col] == name].copy()
            individual_pay_info = individual_pay_info.to_html(index=False).replace('border="1"', 'border="0"')
            
            # Convert the numpy.datetime64 object to a string and extract the date
            date = str(df[df[name_col] == name][payable_date].values[0]).split('T')[0]

            # Create an instance of the Outlook application
            outlook = win32.Dispatch('Outlook.Application')

            # Create a new mail item
            mail = outlook.CreateItem(0)  # 0 represents olMailItem for a new email
            mail.Subject = 'Commission Payment {}'.format(date)
            open_message = "<br><h4></h4>"
            close_message = "<br><h4></h4>"
            mail.HTMLBody = "{}{}{}{}".format(name, open_message, close_message, individual_pay_info)

            # Set the distribution list as the sender (from address)
            mail.SentOnBehalfOfName = 'commissions@companysolar.com'
            # Set the recipient's email address
            mail.To = 'bseljestad@companysolar.com'
            # Send the email
            mail.Send()
    
    manager_name = 'Manager Name'
    setter_name = 'Setter'
    regional_name = 'Regional Name'
    team_lead_name = 'Team Lead Name'
    setter_paydate = 'Payable Date'
    rest_paydate = 'Payable_Date'

    iter_df_send_email(setter_df, setter_name, setter_paydate)
    iter_df_send_email(manager_df, manager_name, rest_paydate)
    iter_df_send_email(team_lead_df, team_lead_name, rest_paydate)
    iter_df_send_email(regional_df, regional_name, rest_paydate)

def main():
    manager_df, team_lead_df, setter_df, regional_df = get_current_paydate_file()
    manager_df, team_lead_df, setter_df, regional_df = set_dfs(manager_df, team_lead_df, setter_df, regional_df)
    emails_df = get_emails_df()
    send_emails(manager_df, team_lead_df, setter_df, regional_df, emails_df)
    print('Success!!!') # Absolutely necessary do not remove

if __name__ == '__main__':
    main()
