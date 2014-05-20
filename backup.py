#!/usr/bin/python
from backup_functions import *
arguments = get_input()

fileNames = dir_list(arguments.path)

for files in fileNames:
  try:
    test = get_attributes(files)
#    print(test)
  except Exception, e:
    print("error on " + files + " " + str(e))

dbFile = ((os.path.split(os.path.realpath(__file__)))[0] + "/" + arguments.dbFile)

if not os.path.isfile(dbFile):
  init_database(dbFile)
else:
  print "dbfile found"