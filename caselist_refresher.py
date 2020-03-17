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

if __name__ == "__main__":
	network_name = initial_prompts('Case List Refresher')
	refresher(network_name)
	
	
