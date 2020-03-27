# import os  
  
# # importing shutil module  
# import shutil  
  
# # path  
# path = 'C:/Users/Rajnish/Desktop/GeeksforGeeks'
  
# # List files and directories  
# # in 'C:/Users/Rajnish/Desktop/GeeksforGeeks'  
# print("Before moving file:")  
# print(os.listdir(path))  
  
  
# # Source path  
# source = 'C:/Users/Rajnish/Desktop/GeeksforGeeks/source'
  
# # Destination path  
# destination = 'C:/Users/Rajnish/Desktop/GeeksforGeeks/destination'
  
# # Move the content of  
# # source to destination  
# dest = shutil.move(source, destination)  
  
# # List files and directories  
# # in "C:/Users / Rajnish / Desktop / GeeksforGeeks"  
# print("After moving file:")  
# print(os.listdir(path))  
  
# # Print path of newly  
# # created file  
# print("Destination path:", dest) 

from os import listdir
from os.path import isfile, join

sc_path = ''

onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))]