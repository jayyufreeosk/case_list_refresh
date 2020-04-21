from functions import *

import time

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

def main2():
    gsheet_url = 'https://docs.google.com/spreadsheets/d/1wsnBd3AHObl4gnUJ2MlwkusuXRoOsQpu2Kx6zGH8bVY/edit#gid=0' # TEST
    # network_name = initial_prompts('Case List Refresher')
    network_name_sub = "Sam's Club"
    wk = worksheet(gsheet_url, network_name_sub)
    gsheet_formatter(wk, network_name_sub)

def main():
    # gsheet_url = 'https://docs.google.com/spreadsheets/d/1-p5-secff5mixTwj2QSqd7rl4twcmZBmQBaR1LrM2Pw/edit#gid=0' # REAL
    gsheet_url = 'https://docs.google.com/spreadsheets/d/1wsnBd3AHObl4gnUJ2MlwkusuXRoOsQpu2Kx6zGH8bVY/edit#gid=0' # TEST
    # network_name = initial_prompts('Case List Refresher')
    network_name = 'Walmart'
    print('Running Step 1')
    file_list = gfile_list_agg() # Aggregates list of PPTX files in GDrive (>750 elements)
    
    # metric_xlsx_migrator() # Migrates all_wm and all_longitudinal_card
    # print('Step 1 completed.')
    # print('============================================')
    # print('Running Step 2')
    # print('============================================')
    
    if network_name == 'All' or network_name == "Sam's Club":
        network_name_sub = "Sam's Club"

        print("Running PPTX migrator for Sam's Club.")
        try: ppt_migrator(network_name_sub)
        except: 
            logging.error("ppt_migrator failed.", exc_info=True)
            print('PPTX migrator failed.')
        file_list = gfile_list_agg()

        print('----------------------------------------')
        print("Running appender for Sam's Club.")
        wk = worksheet(gsheet_url, network_name_sub)
        gsheet_df = gsheet_import_df(wk)
        
        sc_df_raw = all_longitudinal_card_import()
        sc_df_formatted = sc_formatter(sc_df_raw)
        not_list = gsheet_df['Program'].to_list() # Checks for rows to append
        append_df = sc_df_formatted[~sc_df_formatted['Program'].isin(not_list)] # Returns rows to append
        
        if append_df.empty: 
            print('No new SC rows to append. Stopping upload.')
            logging.warning('No new SC rows to append. Stopping upload.')
        else: 
            gsheet_uploader(wk, gsheet_df, append_df)
            gsheet_formatter(wk, network_name_sub)

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

        # print("Running PPTX migrator for Walmart.")
        # try: ppt_migrator(network_name_sub)
        # except: 
        #     logging.error("ppt_migrator failed.", exc_info=True)
        #     print('PPTX migrator failed.')
        # print("Running appender for Walmart.")
        file_list = gfile_list_agg()

        print('----------------------------------------')
        wk = worksheet(gsheet_url, network_name_sub)
        gsheet_formatter(wk, network_name_sub)
        # gsheet_df = gsheet_import_df(wk)

        # wm_df_raw = all_wm_import()
        # wm_df_formatted = wm_formatter(wm_df_raw)
        # not_list = gsheet_df['Program'].to_list() # Checks for rows to append
        # append_df = wm_df_formatted[~wm_df_formatted['Program'].isin(not_list)] # Returns rows to append

        # if append_df.empty:
        #     print('No new wm rows to append. Stopping upload.')
        #     logging.warning('No new WM rows to append. Stopping upload.')
        # else: 
        #     gsheet_uploader(wk, gsheet_df, append_df)
        #     gsheet_formatter(wk, network_name_sub)

        # print('----------------------------------------')
        # print("Running hyperlink updater for Walmart.")
        # try: hyperlink_updater(wk, file_list)
        # except: 
        #     logging.error("hyperlink_updater failed.", exc_info=True)
        #     print('hyperlink_updater failed.')

    logging.info('__main__ complete.')
    print('Step 2 completed.')
if __name__ == "__main__":
    start_time = time.time()
    main()
    seconds = time.time() - start_time
    print('Time Taken:', time.strftime("%H:%M:%S",time.gmtime(seconds)))
