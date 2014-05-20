#!/usr/bin/python
from backup_functions import *
arguments = get_input()

fileNames = dir_list(arguments.path)

for files in fileNames:
  try:
    test = get_attributes(files)
    print(test)
  except Exception, e:
    print("error on " + files + " " + str(e))
