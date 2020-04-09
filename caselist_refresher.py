from functions import *

def initial_prompts(script_name):
    intro = f"""\n
    Welcome to the Freeosk Analytics {script_name}
    """
    print(intro)

    network_choice = """
    Which network to refresh?
    Walmart:     8
    Sam's Club:  4
    All Refresh: 2
    """
    print(network_choice)

    network = input().upper()
    try: network_num = str(network)
    except: network_num = '0'
    
    network_dict = {
                    '8':'Walmart',
                    '4':"Sam's Club",
                    '2':'All'
                    }
    
    while network_num not in network_dict.keys():
        print('The number you entered was not correct.')
        print(network_choice)
        network = input().upper()
        try: network_num = str(network)
        except: network_num = '0'

    network_name = network_dict[network_num]

    return network_name

def main():
    gsheet_url = 'https://docs.google.com/spreadsheets/d/1-p5-secff5mixTwj2QSqd7rl4twcmZBmQBaR1LrM2Pw/edit#gid=0' # REAL
    # gsheet_url = 'https://docs.google.com/spreadsheets/d/1wsnBd3AHObl4gnUJ2MlwkusuXRoOsQpu2Kx6zGH8bVY/edit#gid=0' # TEST
    network_name = initial_prompts('Case List Refresher')
    # network_name = 'All'

    file_list = gfile_list_agg() # Aggregates list of PPTX files in GDrive (>750 elements)

    if network_name == 'All' or network_name == "Sam's Club":
        network_name_sub = "Sam's Club"

        print("Running appender for Sam's Club.")
        wk = worksheet(gsheet_url, network_name_sub)
        gsheet_df = gsheet_import_df(wk)
        not_tuple = gsheet_programs(gsheet_df) # Checks for rows to append
        raw_sc_df = sc_sql_metrics(not_tuple) # Returns rows to append
        if raw_sc_df.empty: 
            print('No new SC rows to append. Stopping upload.')
            logging.warning('No new SC rows to append. Stopping upload.')
        else: 
            sc_df = sc_formatter(raw_sc_df)
            gsheet_uploader(wk, gsheet_df, sc_df)

        print('----------------------------------------')
        print("Running PPTX migrator for Sam's Club.")
        try: ppt_migrator(network_name_sub)
        except: 
            logging.error("ppt_migrator failed.", exc_info=True)
            print('PPTX migrator failed.')
        
        print('----------------------------------------')
        print("Running hyperlink updater for Sam's Club.")
        try: hyperlink_updater(wk, file_list)
        except: 
            logging.error("Sam's Club hyperlink_updater failed.", exc_info=True)
            print('Hyperlink_updater failed.')
        logging.warning('No new SC rows to append. Stopping upload.')

    print('============================================')

    if network_name == 'All' or network_name == "Walmart":
        network_name_sub = 'Walmart'

        print("Running appender for Walmart.")
        wk = worksheet(gsheet_url, network_name_sub)
        gsheet_df = gsheet_import_df(wk)
        not_tuple = gsheet_programs(gsheet_df)
        raw_wm_df = wm_sql_metrics(not_tuple)
        if raw_wm_df.empty:
            print('No new wm rows to append. Stopping upload.')
            logging.warning('No new WM rows to append. Stopping upload.')
        else: 
            wm_df = wm_formatter(raw_wm_df)
            gsheet_uploader("Walmart", gsheet_df, wm_df)

        print('----------------------------------------')
        print("Running PPTX migrator for Walmart.")
        try: ppt_migrator(network_name_sub)
        except: 
            logging.error("ppt_migrator failed.", exc_info=True)
            print('PPTX migrator failed.')

        print('----------------------------------------')
        print("Running hyperlink updater for Walmart.")
        try: hyperlink_updater(wk, file_list)
        except: 
            logging.error("hyperlink_updater failed.", exc_info=True)
            print('hyperlink_updater failed.')

        
        logging.info('__main__ complete.')
if __name__ == "__main__":
    main()