from __future__ import print_function
import pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

import pygsheets

import pandas as pd
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

import time

from config import mysql_credentials

# https://pygsheets.readthedocs.io/en/stable/worksheet.html
def mysql_query(query='DEFAULT', verbose=False):
    import sqlalchemy as sql
    username = mysql_credentials['username']
    password = mysql_credentials['password']
    connection = mysql_credentials['connection']
    database_name = mysql_credentials['database_name']
    connect_string = f'mysql://{username}:{password}@{connection}/{database_name}'
    sql_engine = sql.create_engine(connect_string)
    df = pd.read_sql_query(query, sql_engine)
    if verbose: print(df.shape)
    return df

def worksheet(gsheet_url, network):
    gc = pygsheets.authorize(client_secret='credentials/client_secret.json')
    sh = gc.open_by_url(gsheet_url)

    network_dict = {
    "Sam's Club":0,
    'Walmart':1
    }
    
    network_choice = network_dict[network] # Relating network to sheet index
    # network_choice = 2 # testing
    wk = sh[network_choice]
    return wk


# GOOGLE SPREADSHEET DATAFRAME UPDATER
def gsheet_import_df(wk, verbose=False):

    gsheet_df = wk.get_as_df()
    
    if verbose: print(df.shape)
    
    print(f'Successfully returned {network}.')
    
    return gsheet_df

def gsheet_programs(gsheet_df):
    print(f'Imported Gsheet Dataframe has (rows, columns): {gsheet_df.shape}.')
    not_list = gsheet_df['Program'].to_list() + ['0', '0']
    not_tuple = tuple(not_list)
    print('Tupled dataframe programs.')
    return not_tuple

def sc_sql_metrics(not_tuple):
    query = f"""
    SELECT metrics.program, metrics.weeks_post, metrics.start_date, metrics.end_date, 
    metrics.item_number, metrics.item_description, metrics.category, metrics.subcategory,
    metrics.category_num, metrics.subcat_num, metrics.Freeosk_clubs, metrics.control_clubs,
    metrics.transactions, metrics.scans, metrics.audience, metrics.engagement, metrics.conversion,
    metrics.repeat, metrics.Immediate, metrics.`HH_A%%`, metrics.`HH_B%%`, metrics.`HH_C%%`,
    lift.freeosk_lift, lift.control_lift, cfg.merchandising_type, cfg.placement_type
    FROM longitudinal.c_metrics as metrics
    JOIN longitudinal.c_lift as lift ON metrics.Program = lift.program_name
    JOIN longitudinal.c_cfg as cfg on metrics.program = cfg.program_name
    WHERE metrics.Weeks_post = '12W'
    AND metrics.program NOT IN {not_tuple}
    AND metrics.program LIKE '%%_12.zip%%';
    """
    raw_sc_df = mysql_query(query)
    print(f'Number of SC rows to append: {len(raw_sc_df.index)}')
    return raw_sc_df

def sc_formatter(raw_sc_df):
    # Formatting
    print(f'Successfully returned the MySQL query')
    sc_df = raw_sc_df.copy()
    sc_df['Traffic/club'] = sc_df['audience'] / sc_df['Freeosk_clubs']
    sc_df['Scans/club'] = sc_df['scans'] / sc_df['Freeosk_clubs']
    sc_df['Program2'] = sc_df['program'].str.split("_", n=1, expand=True)[0]
    sc_df['Unique Identifier'] = sc_df['Program2'] + sc_df['item_number'].astype(str)
    renamed_columns = {
        'HH_A%': 'A%',
        'HH_B%': 'B%',
        'HH_C%': 'C%'
    }
    sc_df = sc_df.rename(columns=renamed_columns)
    sc_df.loc[:, 'Tags'] = ''
    sc_df.loc[:, 'Notes'] = ''
    sc_df.columns = [column.replace('_', ' ') for column in list(sc_df)]
    sc_df.columns = [column.capitalize() for column in list(sc_df)]
    print(f'Successfully formatted the MySQL query')
    
    # Mapping to food boolean
    food_mapping = pd.read_excel('data/Food_Mapping.xlsx')
    sc_df_2 = pd.merge(sc_df, food_mapping,  how='left', 
                       left_on=['Category num','Subcat num'], 
                       right_on = ['CATEGORY_NUMBER','SUB_CATEGORY_NUMBER'])
    print(f'Successfully joined to Food_Mapping.xslx')
    
    # Final column rearrangement
    rearranged_columns = ['Program', 'Tags', 'Notes', 'Weeks post', 'Start date', 
                      'End date', 'Item number', 'Item description', 'Category', 
                      'Subcategory', 'Category num', 'Subcat num', 'Freeosk clubs', 
                      'Control clubs', 'Transactions', 'Audience', 'Traffic/club', 
                      'Scans', 'Scans/club', 'Engagement', 'Conversion', 'Repeat', 
                      'Immediate', 'A%', 'B%', 'C%', 'Freeosk lift', 'Control lift', 
                      'Merchandising type', 'Placement type', 'Program2', 'Unique identifier', 'Food']
    
    sc_df_3 = sc_df_2[rearranged_columns].reset_index(drop=True)
    
    print(f'SC_Formatter completed! Shape is {sc_df_3.shape}')
    
    return sc_df_3  

def wm_sql_metrics(not_tuple):
    query = f"""SELECT traceable.Program_name, 
    traceable.Weeks_post, traceable.Start_date, traceable.End_date,
    traceable.Item_number, traceable.Placement_Name, traceable.Dept,
    traceable.Sub_Dept, traceable.Dept_Name, traceable.Sub_Dept_Name,
    traceable.Freeosk_stores, traceable.Rest_of_Chain_Stores,
    traceable.Traceable_Audience, traceable.Non_Traceable_Aud,
    traceable.Audience, traceable.Scans, traceable.Total_Conv,
    traceable.Repeat, traceable.A, traceable.B, traceable.C,
    traceable.Households, traceable.`A%%`, traceable.`B%%`, traceable.`C%%`, traceable.`A+B%%`,
    lift.Control_lift, lift.Freeosk_lift
    FROM longitudinal.traceable as traceable
    JOIN longitudinal.lift as lift ON traceable.program_name = lift.Program_name
    WHERE traceable.Weeks_post = '4W'
    AND traceable.program_name LIKE '%%_4.zip'
    AND traceable.program_name NOT IN {not_tuple};"""
    raw_wm_df = mysql_query(query)
    print(f'Number of WM rows to append: {len(raw_wm_df.index)}')
    return raw_wm_df

def wm_formatter(raw_wm_df):
    # Formatting
    print(f'Successfully returned the MySQL query')
    wm_df = raw_wm_df.copy()
    wm_df['Traffic/store'] = wm_df['Audience'] / wm_df['Freeosk_stores']
    wm_df['Scans/store'] = wm_df['Scans'] / wm_df['Freeosk_stores']
    wm_df['Program2'] = wm_df['Program_name'].str.split("_", n=1, expand=True)[0]
    wm_df.loc[:, 'Tags'] = ''
    wm_df.loc[:, 'Notes'] = ''
    wm_df.columns = [column.replace('_', ' ') for column in list(wm_df)]
    wm_df.columns = [column.capitalize() for column in list(wm_df)]
    print(f'Successfully formatted the MySQL query')
    
    # Final column rearrangement
    rearranged_columns = ['Program name', 'Tags', 'Notes', 'Weeks post', 'Start date', 'End date', 
                          'Item number', 'Placement name', 'Dept', 'Sub dept', 'Dept name', 
                          'Sub dept name', 'Freeosk stores', 'Rest of chain stores', 'Traceable audience', 
                          'Non traceable aud', 'Audience', 'Traffic/store', 'Scans', 'Scans/store', 'Total conv', 
                          'Repeat', 'A', 'B', 'C', 'Households', 'A%', 'B%', 'C%', 'A+b%', 'Control lift', 
                          'Freeosk lift', 'Program2']
    
    wm_df = wm_df[rearranged_columns].reset_index(drop=True)
    
    rename_col = {'A+b%':'A+B%', 'Program name': 'Program'}
    wm_df = wm_df.rename(columns=rename_col)
    print(f'WM_Formatter completed! Shape is {wm_df.shape}')
    
    return wm_df

def gsheet_uploader(wk, gsheet_df, append_df):
    gsheet_import_appended = gsheet_df.append(append_df, ignore_index=True)
    
    wk.clear('A2') # A2 is start of dataframe
    wk.set_dataframe(gsheet_import_appended, 'A2', copy_index=False, copy_head=False, extend=False, fit=True, escape_formulae=True, nan='')
    print('New data has been successfully uploaded!') 


# HYPERLINK FUNCTIONS
def gfile_list_agg():

    # If modifying these scopes, delete the file token.pickle.
    SCOPES = ['https://www.googleapis.com/auth/drive.metadata.readonly']

    """Shows basic usage of the Drive v3 API.
    Prints the names and ids of the first 10 files the user has access to.
    """
    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                '../credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    service = build('drive', 'v3', credentials=creds)

    # Call the Drive v3 API

    page_token = None
    file_list = {}
    page = 0

    while True:
        results = service.files().list(q="'1qQZpAhzmKoR7drBYh8kYWVS6GaMPCxr3' in parents", spaces='drive',
                                              fields='nextPageToken, files(id, name)',
                                              pageToken=page_token).execute()
        items = results.get('files', [])

        for item in items:
            file_list[item['name']] = item['id']

        page_token = results.get('nextPageToken', None)
        if page_token is None: break

    print('Google Drive: PPTX files collected')
    print(f'There are {len(file_list)} PPTX files in the Cases folder.')
    return file_list

def hyperlink_updater(wk, file_list):
    
    n_rows = wk.rows + 1
    for i in range(2, n_rows):
        
        cell = pygsheets.Cell(f'A{i}', worksheet=wk, cell_data=None)
        cell_value = wk.get_value(f'A{i}')
        
        if not cell.formula and cell_value: # Must have value but no hyperlink

            print(f'Cell A{i} requires a hyperlink: {cell_value}')
            
            file_name = cell_value.split('.')[0] + '_case.pptx' # Name to help locate file names
            cell_nbr = f'A{i}'
                        
            file_list_names = list(file_list.keys())
            
            if file_name in file_list_names:
                print(f'Found one {file_name}!')

                file_name_zip = file_name.split('.pptx')[0] + '.zip' # Reference back to original name

                cell.formula = f'=HYPERLINK("https://drive.google.com/file/d/{file_list[file_name]}", "{file_name_zip}")'

                print(f'Replaced {cell_nbr}: {file_name} : {file_list[file_name]}')
                
            else: 
                print(f'COULD NOT REPLACE {cell_nbr}: {file_name}')