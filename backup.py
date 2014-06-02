#!/usr/bin/env python
from backup_functions import *
from dbCreds import *
import platform
import sys
import threading
arguments = get_input()

#this will limit the number of files backed up unless -f is set
breaker = 1000
threads = 6

if not check_table(dbCreds,arguments.vault):
  print "table doesn't exist for vault " + arguments.vault + " creating."
  init_database(dbCreds,arguments.vault)

#check for and display files with duplicate sha256 hashes
if arguments.dupcheck:
  dupes = dup_check(dbCreds, arguments.vault)
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

if len(fileNames) < threads: threads = len(fileNames)

if arguments.verbose: print "getting backup list"
fileNameChunks = split_seq(fileNames,threads)
threads = []
for chunk in fileNameChunks:
  t = threading.Thread(target=get_backup_list, args=(chunk,dbCreds,arguments.verbose,arguments.vault))
  threads.append(t)
  t.start()

for thread in threads:
  thread.join()
if arguments.verbose: print "finished getting backup list"

if arguments.backup:
  backupFiles = []
  for files in fileNames:
    dbFileInfo = lookup_file_by_path(files,dbCreds,arguments.vault)
    if not dbFileInfo['uploaded'] == "TRUE":
      backupFiles.append(dbFileInfo['path'])

  changedFiles = get_changed_list(fileNames,dbCreds,arguments.verbose,arguments.vault)

  print str(len(fileNames)) + " files found"
  print str(len(backupFiles)) + " new files to be backed up"
  print str(len(changedFiles)) + " changed files to be deleted from glacier and backed up"

  if len(backupFiles) > breaker and not arguments.force:
    print str(len(backupFiles)) + " files found, more than breaker. Use -f to force"
    exit(1)

if arguments.backup:
  backupFiles = backupFiles + changedFiles
  if len(changedFiles) > 0:
    print ("deleting changed files")
    if len(changedFiles) < threads: threads = len(changedFiles)
    changedNameChunks = split_seq(changedFiles,threads)
    threads = []
    for chunk in changedNameChunks:
      t = threading.Thread(target=delete_backup_list, args=(chunk,arguments.vault,dbCreds,arguments.verbose))
      threads.append(t)
      t.start()

    for thread in threads:
      thread.join()

  if len(backupFiles) > 0:
    print ("uploading files")
    if len(backupFiles) < threads: threads = len(backupFiles)
    backupFileChunks = split_seq(backupFiles,threads)
    threads = []
    for chunk in backupFileChunks:
      t = threading.Thread(target=upload_glacier_list, args=(chunk,arguments.vault,dbCreds,arguments.verbose))
      threads.append(t)
      t.start()

    for thread in threads:
      thread.join()

exit(0)


