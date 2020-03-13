# This python script creates a framework to easily upload data to google spreadsheets
# Jay 2020-03-06 15:01:58

# Steps
# 1. Download credentials configuration file from https://developers.google.com/sheets/api/quickstart/python
# 2. pip install --upgrade google-api-python-client google-auth-httplib2 google-auth-oauthlib
# 3. Run python to authenticate at least once. Try not to change the scopes
# 4. Input the url into url_id_splitter and the sheet name

# SAMPLE PYTHON SCRIPT FROM https://developers.google.com/sheets/api/quickstart/python

def url_id_splitter(url): return url.split('/')[5]

def google_sheet_to_pandas(gsheet):
    import pandas as pd
    """ Converts Google sheet data to a Pandas DataFrame.
    Note: This script assumes that your data contains a header file on the first row!
    Also note that the Google API returns 'none' from empty cells - in order for the code
    below to work, you'll need to make sure your sheet doesn't contain empty cells,
    or update the code to account for such instances.
    """
    header = gsheet.get('values', [])[0]   # Assumes first line is header!
    values = gsheet.get('values', [])[1:]  # Everything else is data.
    if not values: print('No data found.')
    else:
        all_data = []
        for col_id, col_name in enumerate(header):
            column_data = []
            for row in values:
                column_data.append(row[col_id])
            ds = pd.Series(data=column_data, name=col_name)
            all_data.append(ds)
        df = pd.concat(all_data, axis=1)
        return df

def get_google_sheet(url, sheet_name = "Sheet1"):
    import pickle
    import os.path
    from googleapiclient.discovery import build
    from google_auth_oauthlib.flow import InstalledAppFlow
    from google.auth.transport.requests import Request
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly'] # If modifying these scopes, delete the file token.pickle.
    URL_ID = url_id_splitter(url)
    
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
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    service = build('sheets', 'v4', credentials=creds)

    sheet = service.spreadsheets() # Call the Sheets API

    result = sheet.values().get(spreadsheetId=URL_ID,
                                range=sheet_name).execute()
    # values = result.get('values', []) # Uncomment to grab values as lists
    # values = result.get('values', [])
    # print(values)
    # if not values:
    #     print('No data found.')
    # else:
    #     print('Name, Major:')
    #     for row in values:
    #         # Print columns A and E, which correspond to indices 0 and 4.
    #         print('%s, %s' % (row[0], row[1]))
    return result

def main():
    # https://medium.com/game-of-data/play-with-google-spreadsheets-with-python-301dd4ee36eb
    import pygsheets
    gc = pygsheets.authorize(client_secret='client_secret.json')
    sh = gc.open_by_url('https://docs.google.com/spreadsheets/d/1wsnBd3AHObl4gnUJ2MlwkusuXRoOsQpu2Kx6zGH8bVY/edit#gid=0')
    wks = sh[0]
    df = wks.get_as_df()
    print(df)
    # result = get_google_sheet('https://docs.google.com/spreadsheets/d/1wsnBd3AHObl4gnUJ2MlwkusuXRoOsQpu2Kx6zGH8bVY/edit#gid=0')
    # df = google_sheet_to_pandas(result)
    # print(f"""
    # Google Spreadsheet successfully returned. Shape: {df.shape}, Columns: {list(df)}.
    # Exiting now.
    # """)

if __name__ == '__main__':
    main()
