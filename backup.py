#!/usr/bin/env python
from backup_functions import *
import platform
import sys
arguments = get_input()

#create database where we store all file and upload info
if platform.system() == "Windows":
  dbFile = (os.path.dirname(os.path.realpath(sys.argv[0])) + "\\" + arguments.dbFile)
else:
  dbFile = (os.path.dirname(os.path.realpath(sys.argv[0])) + "/" + arguments.dbFile)
if not os.path.isfile(dbFile):
  init_database(dbFile)

#check for and display files with duplicate md5 hashes
if arguments.dupcheck:
  dupes = dup_check(dbFile)
  for key, value in dupes.iteritems():
    print (key)
    for files in value:
      print "  " + files[0] + ",  " + str(files[1]) + "  bytes"
  exit(0)

#find all files in path, this can take some time
start = time.time()
if arguments.verbose: print("walking directory tree") 
fileNames = dir_list(arguments.path)
end = time.time() - start
if arguments.verbose: print("took ") + str(end) + (" seconds")

for files in fileNames:
  try:
    fileInfo = lookup_file_by_path(files,dbFile)
    if fileInfo is None:
      start = time.time()
      attributes = get_attributes(files)
      end = time.time() - start
      if arguments.verbose: print("finished in ") + str(end) + (" seconds")
      insert_file(attributes,dbFile)
  except Exception, e:
    print("error on " + files + " " + str(e))


