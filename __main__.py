import pandas as pd
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


sc_df = SC_formatter()
print('------------------')
gsheet_import_sc_appended = sc_gsheet_appender(sc_df)
print('------------------')
sc_gsheet_uploader(gsheet_import_sc_appended)