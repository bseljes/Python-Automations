import pyodbc, os, smtplib, mysql.connector, logging, requests
import pandas as pd
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime, timedelta
from dateutil.parser import parse
import sys, time, json, re
# from bs4 import BeautifulSoup

'''
This module will connect to multiple databases and apps used by the company to be used with python automations.
'''
class NewDBCon:
    '''Connects to Azure SQL Database run_query returns a pd.df'''
    def __init__(self):
        # Define the connection string
        self.connection_string = (
            'connection_string'
        )
    def run_query(self, query):
        # Establish the database connection
        self.connection = pyodbc.connect(self.connection_string)
        # Execute SQL query
        self.new_df = pd.read_sql(query , self.connection)
        self.connection.close()
        return self.new_df

class OldDBCon:
    def __init__(self):
        self.connection = mysql.connector.connect(
            'connection_string'
        )
    def run_query(self, query):
        self.cursor = self.connection.cursor()
        self.cursor.execute(query)
        self.rows = self.cursor.fetchall()
        self.column_names = [desc[0] for desc in self.cursor.description]
        self.old_df = pd.DataFrame(self.rows, columns= self.column_names)
        self.cursor.close()
        self.connection.close()
        return self.old_df
   
    def run_update_query(self, query):
        cursor = self.connection.cursor()
        try:
            cursor.execute(query)
            self.connection.commit()
        finally:
            cursor.close()
        return

class EmailSender:
    '''This uses SMTP Authentification.  It can take mulitple attachments of files.'''
    def __init__(self, from_email, password):
        self.smtp_server = 'smtp-mail.outlook.com'
        self.smtp_port = 587
        self.from_email = from_email
        self.password = password
    
    def send_email(self, to_email, subject, open_message, close_message=' ', attachment_paths=['None'], attachment_names=None, from_distribution=None, cc_email_list=None, table_html=None):
        try:
            # Create a multipart message container
            msg = MIMEMultipart()
            msg['From'] = from_distribution if from_distribution else self.from_email
            msg['To'] = to_email
            msg['Subject'] = subject
            if cc_email_list != None:
                msg['Cc'] =  ', '.join(cc_email_list)
            
            message = open_message + close_message
            # Attach HTML table body if provided
            if table_html:
                html_body = f"<p>{open_message}</p><p>{table_html}</p><p>{close_message}</p>"
                msg.attach(MIMEText(html_body, 'html'))
            else:
                msg.attach(MIMEText(message, 'plain'))

            if not attachment_names:
                attachment_names = attachment_paths

            if attachment_paths != ['None']:
                for path, name in zip(attachment_paths, attachment_names):
                    extension = path[-5:].split('.')[-1]
                    if not name.endswith(extension):
                        name = name + '.' + extension
                    with open(path, 'rb') as file:
                        attachment = MIMEBase('application', 'octet-stream')
                        attachment.set_payload(file.read())
                    encoders.encode_base64(attachment)
                    attachment.add_header('Content-Disposition', f'attachment; filename="{name}"')
                    msg.attach(attachment)


            
            # Connect to SMTP server and send email
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.from_email, self.password)
                server.send_message(msg)

            
            print('Success! Email with subject "{}" and {} attachments sent to {}'.format(subject, len(attachment_paths), to_email))  
        except Exception as e:
            print(f'Could not send email to {to_email}: {e}')

class PodioAPI:
    def __init__(self, base_url,  username, password, client_id, client_secret):
        self.base_url = base_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.username = username
        self.password = password
        self.access_token = self.get_access_token()
    
    def get_access_token(self):
        auth_url = self.base_url + 'oauth/token'
        print('Getting access token')
        response = requests.post(auth_url, data={
            'grant_type': 'password',
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'username': self.username,
            'password': self.password
        })
        if response.status_code == 200:
            print('Access Token granted.')
            return response.json().get('access_token')
        else:
            print(f'Access Token not received.\n{response}')

        auth_url = self.base_url + 'oauth/token/v2'
        print('Getting access token')
        response = requests.post(auth_url, data={
            'grant_type': 'password',
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'username': self.username,
            'password': self.password
        })
        if response.status_code == 200:
            print('Access Token granted.')
            return response.json().get('access_token')
        else:
            print(f'Access Token not received.\n{response}')

    def verify_webhook(self, hook_id):
        if not self.access_token:
            self.access_token = self.get_password_access_token()

        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json'
        }
        url = f'{self.base_url}hook/{hook_id}/verify/request'
        data = json.dumps({'type': 'hook.verify', 'hook_id': hook_id})
        response = requests.post(url, headers=headers, data=data)
        print(response)
        return response
    
    def validate_webhook(self, hook_id, code):
        if not self.access_token:
            self.access_token = self.get_password_access_token()

        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json'
        }
        url = f'{self.base_url}hook/{hook_id}/verify/request'

    def get_hooks(self, ref_type, ref_id):
        if not self.access_token:
            self.access_token = self.get_password_access_token()
        
        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json'
        }

        url = f'{self.base_url}hook/{ref_type}/{ref_id}'
        response = 1

    def update_podio_field(self, item_id, field_id, new_value):
        if not self.access_token:
            self.access_token = self.get_password_access_token()
        
        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json'
        }
        
        # Update the field value
        url = f'{self.base_url}item/{item_id}/value/{field_id}'
        data = json.dumps({'value': new_value})
        
        response = requests.put(url, headers=headers, data=data)
        
        if response.status_code == 200:
            print(f"Successfully updated item {item_id} with new value: {new_value}")
        else:
            print(f"Failed to update item {item_id}. Response: {response.json()}")

    def clean_items(self, items):
        items = items['items']
        if not items:
            return None
        else:
            items_dict = {}
            for item in items:
                podio_app_item_id = self.formatted_app_id + '.' + item['app_item_id_formatted']
                fields = item['fields']
                fields_dict = {}
                for field in fields:
                    label = field['label']
                    type = field['type']
                    values = 'unable to get value'
                    match type:
                        case 'app':
                            if 'title' in field['values'][0]['value']['app'].keys():
                                values = field['values'][0]['value']['app']['title']
                            elif 'title' in field['values'][0]['value'].keys():
                                values = field['values'][0]['value']['title']
                            else:
                                values = ''
                        case 'embed':
                            values = ''
                        case 'date':
                            values = field['values'][0]['start_date']
                        case 'category':
                            values = field['values'][0]['value']['text']
                        case _: 
                            if 'value' in field['values'][0].keys():
                                values = field['values'][0]['value']
                            elif 'start_date' in field['values'][0].keys():
                                values = field['values'][0]['start_date']
                    field_dict = {
                        'label': label,
                        'type': type,
                        'values': values
                        }
                    fields_dict[label] = field_dict
                items_dict[podio_app_item_id] = fields_dict # appID.itemID
            return items_dict

    def get_filtered_items_v2(self, app_id, filters):
        if not self.access_token:
            self.access_token = self.get_password_access_token()
        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json'
        }
        url = f'{self.base_url}item/app/{app_id}/filter/'
        limited = 'limit' in filters.keys()
        limit = filters.pop('limit', 500)  # Default limit to 500 if not specified
        offset = 0

        all_items = {}
        while True:
            print('api sent')
            response = requests.post(url, headers=headers, json={'filters': filters, 'limit': limit, 'offset': offset})
            print('reponse received')
            if response.status_code == 200:
                items = response.json()
                if len(items['items']) > 0 :
                    cleaned_items = items
                    all_items.update(cleaned_items)
                    offset += len(cleaned_items)
                else:
                    cleaned_items = []

                # Break if the number of items is less than the limit (all items gathered) or if response is limited
                if len(cleaned_items) < limit or limited:
                    return all_items

    def get_filtered_items(self, app_id, filters):
        self.formatted_app_id = str(app_id)
        # if not self.access_token:
        #     logging.error("Getting new access token")
        #     self.access_token = self.get_password_access_token()

        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json'
        }
        url = f'{self.base_url}item/app/{app_id}/filter/'
        limited = 'limit' in filters.keys()
        limit = filters.pop('limit', 500)  # Default limit to 500 if not specified
        offset = 0

        all_items = {}
        while True:
            response = requests.post(url, headers=headers, json={'filters': filters, 'limit': limit, 'offset': offset})

            if response.status_code == 200:
                items = response.json()
                if len(items['items']) > 0 :
                    cleaned_items = self.clean_items(items)
                    all_items.update(cleaned_items)
                    offset += len(cleaned_items)
                else:
                    cleaned_items = []

                # Break if the number of items is less than the limit (all items gathered) or if response is limited
                if len(cleaned_items) < limit or limited:
                    return all_items

            elif response.json()['error'] == 'rate_limit':  # Rate limit exceeded
                print('Rate limit exceeded.  Sleeping for 300 seconds.')
                number = 0
                while number <= 300:
                    sys.stdout.write(f'\r{number}/300')
                    time.sleep(1)
                    sys.stdout.flush()
                    number += 1
            elif response.json()['error_description'] == 'expired_token':
                self.access_token = self.get_password_access_token()

            else:
                logging.error(f"Failed to retrieve items: {response.json()}\n"
                            f"PARAMETERS\nFILTERS: {filters}\nLIMIT: {limit}\nOFFSET: {offset}\nRESPONSE: {response.json()}")
                return None

    def get_org(self, org_id):
        url = f'{self.base_url}org/{org_id}/all_spaces'
        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json'
        }
        response = requests.get(url, headers=headers)
        return response.json()

    def item_cleaner(self, item):
        clean_dict = {}
        for field in item:
            skip = 0
            field_id = field['field_id']
            label = field['label']
            field_type = field['type']
            values = field['values']
            match field_type:
                case 'date':
                    value = ','.join([value['start'] for value in values])
                case 'contact':
                    value = ','.join([value['value']['name'] for value in values])
                case 'text':
                    value = ','.join([re.sub(r'<.*?>','',(value['value'])) for value in values])
                case 'category':
                    value = ','.join([value['value']['text'] for value in values])
                case 'app':
                    value = ','.join([f"{str(value['value']['app']['app_id'])}.{str(value['value']['item_id'])}" for value in values])
                case 'phone' | 'email' | 'number' | 'location':
                    value = ','.join([value['value'] for value in values])
                case 'calculation':
                    skip = 1
                    # if 'start' in values[0].keys():
                    #     value = ','.join([value['start'] for value in values])
                    # else:
                    #     try:
                    #         value = [parse(value['value']).strftime('%Y-%m-%d') for value in values]
                    #         value = ','.join(value)
                    #     except:
                    #         value = [value['value'] for value in values]
                    #         float_values = []
                    #         for v in value:
                    #             try:
                    #                 float_values.append(float(v))
                    #             except ValueError:
                    #                 float_values.append(v)
                            # value = float_values[0]
                case 'money':
                    value = float(values[0]['value'])
                case _:
                    skip = 1
                    pass # Error logic to send email to CRM Admin for fix
            if not skip:
                item_dict = {
                    'field_id': field_id,
                    'field_type': field_type,
                    'field_label': label,
                    'field_value': value
                }
                clean_dict[field_id] = item_dict
        return clean_dict

    def get_podio_item_values(self, item_id):
        def cleaner(response_items):
            clean_dict = {}
            for item in response_items:
                skip = 0
                field_id = item['field_id']
                label = item['label']
                field_type = item['type']
                values = item['values']
                match field_type:
                    case 'date':
                        value = ','.join([value['start'] for value in values])
                    case 'contact':
                        value = ','.join([value['value']['name'] for value in values])
                    case 'text':
                        value = ','.join([re.sub(r'<.*?>','',(value['value'])) for value in values])
                    case 'category':
                        value = ','.join([value['value']['text'] for value in values])
                    case 'app':
                        value = ','.join([f"{str(value['value']['app']['app_id'])}.{str(value['value']['item_id'])}" for value in values])
                    case 'phone' | 'email' | 'number' | 'location':
                        value = ','.join([value['value'] for value in values])
                    case 'calculation':
                        skip = 1
                        # if 'start' in values[0].keys():
                        #     value = ','.join([value['start'] for value in values])
                        # else:
                        #     try:
                        #         value = [parse(value['value']).strftime('%Y-%m-%d') for value in values]
                        #         value = ','.join(value)
                        #     except:
                        #         value = [value['value'] for value in values]
                        #         float_values = []
                        #         for v in value:
                        #             try:
                        #                 float_values.append(float(v))
                        #             except ValueError:
                        #                 float_values.append(v)

                        #         value = float_values[0]
                    case 'money':
                        value = float(values[0]['value'])
                    case _:
                        skip = 1
                        pass # Error logic to send email to CRM Admin for fix
                if not skip:
                    item_dict = {
                        'field_id': field_id,
                        'field_type': field_type,
                        'field_label': label,
                        'field_value': value
                    }
                    clean_dict[field_id] = item_dict
            return clean_dict
        
        if not self.access_token:
            self.access_token = self.get_password_access_token()
        
        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json'
        }
        
        url = f'{self.base_url}item/{item_id}/value'
        
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            return cleaner(response.json())  # Returns the item values as a JSON object
        else:
            print(f"Failed to get item {item_id}. Response: {response.json()}")
            return None

    def create_hook(self, ref_type, ref_id, event_type):
        # self.access_token = self.get_app_access_token()
        data = {
            'ref_type': ref_type,
            'ref_id': ref_id,
            'type': event_type,
            'url': 'https://23ec-97-75-169-106.ngrok-free.app'
        }
        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json'
        }
        url = f'{self.base_url}hook/{ref_type}/{ref_id}/'
        response = requests.post(url, headers=headers, json=data)
        return response.json()

    def get_org(self):
        # Non resource intensive.  Can run 1000 API calls an hour no per minute/5minute rates.
        url = f'{self.base_url}org/{self.org_id}/all_spaces'
        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json'
        }
        response = requests.get(url, headers=headers)
        self.data_size += len(response.content)
        self.api_count += 1
        return response.json()

    def get_apps_in_space(self, space_id):
        # Non resource intensive.  Can run 1000 API calls an hour no per minute/5minute rates.
        url = self.base_url + f'app/space/{space_id}/'
        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json'
        }
        space_response = requests.get(url, headers=headers)
        self.api_count += 1
        apps = space_response.json()
        data = []
        for app in apps:
            if space_response.status_code == 200:
                space_app_id = str(app['space_id']) + '.' + str(app['app_id'])
                app_name = app['config']['name']
                tuple = (space_app_id, app_name)
                data.append(tuple)
        return data
    
    def get_app_fields_data(self, app_id):
        # Non resource intensive.  Can run 1000 API calls an hour no per minute/5minute rates.
        url = self.base_url + f'app/{app_id}'
        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json'
        }
        response = requests.get(url, headers=headers)
        self.data_size += len(response.content)
        self.api_count += 1
        if 'fields' in response.json().keys():
            fields = response.json()['fields']
            fields_info = {}
            for field in fields:
                field_hidden = field['config']['hidden']
                field_id = field['field_id']
                field_label = field['label']
                field_type = field['type']
                if 'return_type' in field.keys():
                    field_return_type = field['return_type']
                else:
                    field_return_type = field_type
                fields_info[field_id] = {'field_label': field_label, 'field_id': field_id, 'hidden': field_hidden, 'type': field_type, 'return_type': field_return_type}
        else:
            fields_info = {}
        return fields_info

    def get_podio_system_setup(self):
        print('Getting spaces in organization')
        org_response = self.get_org()  # Getting spaces in organization
        org_info = {}
        for space in org_response:
            space_id = space['space_id']
            if 'name' in space.keys():
                space_name = space['name']
                if space_name != 'name_space':
                    print(f'Getting apps in space: {space_name}')
                    app_response = self.get_apps_in_space(space_id)  # Getting apps in space
                    for app in app_response:
                        print(f'Getting fields in app: {space_name}/{app[1]}')
                        space_app_id, app_name = app
                        space_id, app_id = space_app_id.split('.')
                        if space_name not in org_info:
                            org_info[space_name] = {}
                        org_info[space_name][app_name] = {
                            'space_app_id': space_app_id,
                            'app_id': app_id,
                            'fields': self.get_app_fields_data(app_id)
                        } # Getting field info in app
        return org_info