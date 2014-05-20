#!/usr/bin/python
from backup_functions import *
arguments = get_input()

fileNames = dir_list(arguments.path)

for i in range(0, 100):
  try:
    test = get_attributes(fileNames[i])
    print(test)
  except:
    print("error on " + fileNames[i])
