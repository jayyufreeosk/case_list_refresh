# Case list refresh

## Directories
- data: houses the temporary and example outputs
- notebook: houses the experimental notebooks pre-production

## Python

`functions.py` houses all the functions necessary for `caselist_refresher.py` and `caselist_refrsher_auto.py` to run

## Google API Startup

1. Follow instructions for pygsheets authorization: https://pygsheets.readthedocs.io/en/stable/authorization.html.
2. Follow steps 1 and 2 for GDive authorization: https://developers.google.com/drive/api/v3/quickstart/python.
3. Fill out `config_template.py`.
4. Create and sync with Google Drive cases folder in local

## Directions

1. Make sure to edit the config file to reflect mysql credentials and source/destination directories.
2. Make sure to have `Food_Mapping.xlsx` in the data directory. If in main directory, relocate the file into data.
3. Run either `caselist_refresher.py` or `caselist_refresher_auto.py` to start the process.
4. Check `app.log` to see if any errors come up.
5. If using the notebook, make sure to import the config file for proper use.
