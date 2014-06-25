#!/usr/bin/env python
from backup_functions import *
from dbCreds import *
import platform
import sys
import threading
import Queue
arguments = get_input()

#this will limit the number of files backed up unless -f is set
breaker = 5000
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

queue = Queue.Queue()
activeThreads = []

if arguments.verbose: print "getting backup list"

for fileName in fileNames:
  queue.put(fileName)

for i in range(threads):
  t = threading.Thread(target=get_backup_list, args=(queue,dbCreds,arguments.verbose,arguments.vault))
  activeThreads.append(t)
  t.start()

for activeThread in activeThreads:
  activeThread.join()

if arguments.backup:
  backupFiles = []
  changedFiles = []
  for files in fileNames:
    dbFileInfo = lookup_file_by_path(files,dbCreds,arguments.vault)
    if not dbFileInfo['uploaded'] == "TRUE":
      backupFiles.append(dbFileInfo['path'])

  if arguments.verbose: print "getting changed list"

  for fileName in fileNames:
    queue.put(fileName)
  for i in range(threads):
    t = threading.Thread(target=get_changed_list, args=(queue,fileNames,dbCreds,arguments.verbose,arguments.vault,changedFiles))
    activeThreads.append(t)
    t.start()

  for activeThread in activeThreads:
    activeThread.join()

  print str(len(fileNames)) + " files found"
  print str(len(backupFiles)) + " new files to be backed up"
  print str(len(changedFiles)) + " changed files to be deleted from glacier and backed up"

  if len(backupFiles) > breaker and not arguments.force:
    print str(len(backupFiles)) + " files found, more than breaker. Use -f to force"
    exit(1)

if arguments.backup:
  backupFiles = backupFiles + changedFiles
  #if we're using glacier we need to send a delete for changed files and reupload
  #on S3 we can just reupload and the versioned container will clean up
  if len(changedFiles) > 0 and arguments.backup == "glacier":
    print ("deleting changed files")
    if len(changedFiles) < threads: threads = len(changedFiles)

    activeThreads = []
   
    for fileName in changedFiles:
      queue.put(fileName)
   
    for i in range(threads):
      t = threading.Thread(target=delete_backup_list, args=(queue,arguments.vault,dbCreds,arguments.verbose))
      activeThreads.append(t)
      t.start()

    for activeThread in activeThreads:
      activeThread.join()

  if len(backupFiles) > 0:
    print ("uploading files")
    if len(backupFiles) < threads: threads = len(backupFiles)

    activeThreads = []
   
    for fileName in backupFiles:
      queue.put(fileName)
   
    for i in range(threads):
      if arguments.backup == "glacier":
        t = threading.Thread(target=upload_glacier_list, args=(queue,arguments.vault,dbCreds,arguments.verbose))
      elif arguments.backup == "s3":
        t = threading.Thread(target=upload_s3_list, args=(queue,arguments.vault,dbCreds,arguments.verbose))

      activeThreads.append(t)
      t.start()

    for activeThread in activeThreads:
      activeThread.join()

print "Finished backups"
exit(0)


