#!/usr/bin/python
from backup_functions import *
arguments = get_input()

#create database where we store all file and upload info
dbFile = ((os.path.split(os.path.realpath(__file__)))[0] + "/" + arguments.dbFile)
if not os.path.isfile(dbFile):
  init_database(dbFile)

#find all files in path, this can take some time
fileNames = dir_list(arguments.path)

for files in fileNames:
  try:
    fileInfo = lookup_file(files,dbFile)
    if fileInfo is None:
      attributes = get_attributes(files)
      insert_file(attributes,dbFile)
#    print(test)
  except Exception, e:
    print("error on " + files + " " + str(e))


