import pygsheets
import pandas as pd
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

# https://pygsheets.readthedocs.io/en/stable/worksheet.html
def mysql_query(query='DEFAULT', verbose=False):
    import sqlalchemy as sql
    username = 'jyu'
    password = 't4pu6["Hw+X@=~gL'
    connection = '192.168.157.106'
    database_name = 'ods'
    connect_string = f'mysql://{username}:{password}@{connection}/{database_name}'
    sql_engine = sql.create_engine(connect_string)
    df = pd.read_sql_query(query, sql_engine)
    if verbose: print(df.shape)
    return df

def gsheet_import_caselist(network, verbose=False):
    # Relating network to sheet index
    network_dict = {
        'Sams Club':0,
        'Walmart':1
    }
    network_choice = network_dict[network]
    
    gc = pygsheets.authorize(client_secret='client_secret.json')
    sh = gc.open_by_url('https://docs.google.com/spreadsheets/d/1wsnBd3AHObl4gnUJ2MlwkusuXRoOsQpu2Kx6zGH8bVY/edit#gid=0')
    df = sh[network_choice].get_as_df()
    
    if verbose: print(df.shape)
    
    print(f'Successfully returned {network}.')
    
    return df

def SC_metrics_pull():
    query = """
    SELECT metrics.program, metrics.weeks_post, metrics.start_date, metrics.end_date, 
    metrics.item_number, metrics.item_description, metrics.category, metrics.subcategory,
    metrics.category_num, metrics.subcat_num, metrics.Freeosk_clubs, metrics.control_clubs,
    metrics.transactions, metrics.scans, metrics.audience, metrics.engagement, metrics.conversion,
    metrics.repeat, metrics.Immediate, metrics.`HH_A%%`, metrics.`HH_B%%`, metrics.`HH_C%%`,
    lift.freeosk_lift, lift.control_lift, cfg.merchandising_type, cfg.placement_type
    FROM longitudinal.c_metrics as metrics
    JOIN longitudinal.c_lift as lift ON metrics.Program = lift.program_name
    JOIN longitudinal.c_cfg as cfg on metrics.program = cfg.program_name
    WHERE metrics.Weeks_post = '12W';
    """
    raw_sc_df = mysql_query(query)
    return raw_sc_df

def SC_formatter(raw_sc_df=SC_metrics_pull()):
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
    food_mapping = pd.read_excel('Food_Mapping.xlsx')
    sc_df_2 = pd.merge(sc_df, food_mapping,  how='left', 
                       left_on=['Category num','Subcat num'], 
                       right_on = ['CATEGORY_NUMBER','SUB_CATEGORY_NUMBER'])
    print(f'Successfully joined to Food_Mapping.xslx')
    
    # Removing duplicates
    sc_df_3 = sc_df_2[sc_df_2['Program'].str.contains('_12.zip')]
    
    # Final column rearrangement
    rearranged_columns = ['Program', 'Tags', 'Notes', 'Weeks post', 'Start date', 
                      'End date', 'Item number', 'Item description', 'Category', 
                      'Subcategory', 'Category num', 'Subcat num', 'Freeosk clubs', 
                      'Control clubs', 'Transactions', 'Audience', 'Traffic/club', 
                      'Scans', 'Scans/club', 'Engagement', 'Conversion', 'Repeat', 
                      'Immediate', 'A%', 'B%', 'C%', 'Freeosk lift', 'Control lift', 
                      'Merchandising type', 'Placement type', 'Program2', 'Unique identifier', 'Food']
    
    sc_df_3 = sc_df_3[rearranged_columns].reset_index(drop=True)
    
    print(f'SC_Formatter completed! Shape is {sc_df_3.shape}')
    
    return sc_df_3

def sc_gsheet_appender(sc_df):
    
    gsheet_import_sc_df = gsheet_import_caselist('Sams Club')
    
    print(f'Sams Club Dataframe has (rows, columns): {sc_df.shape}')
    print(f'Imported Gsheet Dataframe has (rows, columns): {gsheet_import_sc_df.shape}')
    
    # Checks for rows to append without edits to previous rows
    sc_df_programlist = sc_df['Program'].to_list()
    gsheet_import_programlist = gsheet_import_sc_df['Program'].to_list()
    to_append = [x for x in sc_df_programlist if x not in gsheet_import_programlist]
    print('Rows to append: ' + str(len(to_append)))
    
    gsheet_import_sc_df = gsheet_import_sc_df.set_index('Program')
    sc_df = sc_df.set_index('Program')
    
    gsheet_import_sc_appended = gsheet_import_sc_df.append(sc_df.loc[to_append, :], sort=False)
    gsheet_import_sc_appended = gsheet_import_sc_appended.reset_index()
    print('New sheet Dataframe has (rows, columns): ' + str(gsheet_import_sc_appended.shape))
    
    return gsheet_import_sc_appended

def sc_gsheet_uploader(gsheet_import_sc_appended):
    gc = pygsheets.authorize(client_secret='client_secret.json')
    sh = gc.open_by_url('https://docs.google.com/spreadsheets/d/1wsnBd3AHObl4gnUJ2MlwkusuXRoOsQpu2Kx6zGH8bVY/edit#gid=0')
    sh[0].clear('A2')
    sh[0].set_dataframe(gsheet_import_sc_appended, 'A2', copy_index=False, copy_head=False, extend=False, fit=True, escape_formulae=True, nan='')
    print('New data has been succesfully uploaded!')