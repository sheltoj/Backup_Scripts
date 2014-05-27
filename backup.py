#!/usr/bin/env python
from backup_functions import *
import platform
import sys
arguments = get_input()

#this will limit the number of files backed up unless -f is set
breaker = 1000

#create database where we store all file and upload info
if platform.system() == "Windows":
  dbFile = (os.path.dirname(os.path.realpath(sys.argv[0])) + "\\" + arguments.dbFile)
else:
  dbFile = (os.path.dirname(os.path.realpath(sys.argv[0])) + "/" + arguments.dbFile)
if not os.path.isfile(dbFile):
  init_database(dbFile)

#check for and display files with duplicate sha256 hashes
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

backupFiles = get_backup_list(fileNames,arguments.dbFile,arguments.verbose)

#for files in fileNames:
#  print "keeping " + files
if len(backupFiles) > breaker and not arguments.force:
  print str(len(backupFiles)) + " files found, more than breaker. Use -f to force"
  exit(1)

if arguments.backup:
  for key, value in backupFiles.iteritems():
    print "uploading " + files
    response = upload_glacier(files,arguments.vault,dbFile)
    print "upload finished at " + str(response[1]) + " Mbps with archiveID: " + response[0]









