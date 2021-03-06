#!/usr/bin/env python
import os
import math
import argparse
import errno
import time
import collections
import platform
import boto
import boto.glacier.utils
import json
import random
import MySQLdb as mdb
import MySQLdb.cursors
import Queue
from scandir import *
from filechunkio import FileChunkIO

def get_input():
  parser = argparse.ArgumentParser(description='Glacier backup and dupcheck script')
  parser.add_argument(
                      '-d',
                      action='store',
                      dest='path',
                      help='Directory to dupcheck'
                      )
  parser.add_argument(
                      '--dupcheck',
                      action='store_true',
                      dest='dupcheck',
                      help='only do a dupcheck against the db'
                      )
  parser.add_argument(
                      '-v',
                      action='store_true',
                      dest='verbose',
                      help='enable verbose logging'
                      )
  parser.add_argument(
                      '-b',
                      action='store',
                      dest='backup',
                      help='backup data, options are s3 or glacier'
                      )
  parser.add_argument(
                      '-V',
                      action='store',
                      dest='vault',
                      help='vault to store backups in'
                      )
  parser.add_argument(
                      '-f',
                      action='store_true',
                      dest='force',
                      help='force run in case breaker is tripped'
                      )

  results = parser.parse_args()
  if not (results.path) and not (results.dupcheck):
    parser.error('no path provided')
  return results

def dir_list(path):
  fileNames = []
  for root, dirs, files in walk(path):
    if files:
      for item in files:
        if platform.system() == "Windows":
          fileNames.append(root + "\\" + item)
        else:
          fileNames.append(root + "/" + item)
  return fileNames

def get_attributes(fileName,verbosity):
  if verbosity: print("getting attributes for " + fileName)
  attributes = {}
  attributes['path'] = fileName
  attributes['name'] = os.path.basename(fileName)
  attributes['size'] = os.path.getsize(fileName)

  fileObj = open(fileName,'rb')
  hashes = boto.glacier.utils.compute_hashes_from_fileobj(fileObj)
  fileObj.close()
  attributes['sha256'] = hashes[0]
  attributes['x_amz_sha256_tree'] = hashes[1]
  attributes['modification_time'] = os.stat(fileName).st_mtime
  return attributes	

def check_table(dbCreds,vault):
  conn = mdb.connect(dbCreds['server'] , dbCreds['user'], dbCreds['password'], dbCreds['database']);
  c = conn.cursor()
  c.execute('''SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '%s' ''' % ( vault ) )
  if c.fetchone()[0] == 1:
    conn.close()
    return True

  conn.close()
  return False

def init_database(dbCreds,vault):
  conn = mdb.connect(dbCreds['server'] , dbCreds['user'], dbCreds['password'], dbCreds['database']);
  c = conn.cursor()
  c.execute('''CREATE TABLE %s (
                                   path char(255) UNIQUE NOT NULL,
                                   name char(128),
                                   size BIGINT,
                                   sha256 char(255),
                                   x_amz_sha256_tree char(255),
                                   modification_time DATETIME,
                                   uploaded char(32),
                                   uploaded_at DATETIME,
                                   x_amz_archive_id char(255),
                                   vault char(128)
                                  ) TYPE=innodb
            ''' % ( vault ) )
  conn.commit
  conn.close

def insert_file(attributes,dbCreds,vault):
  conn = mdb.connect(dbCreds['server'] , dbCreds['user'], dbCreds['password'], dbCreds['database']);
  c = conn.cursor()
  c.execute('''BEGIN''')
  c.execute('''INSERT INTO {} (path, name, size, sha256, x_amz_sha256_tree, modification_time)
           VALUES (%s,%s,%s,%s,%s,FROM_UNIXTIME(%s))'''.format(vault), 
           ( attributes['path'], attributes['name'], attributes['size'] , attributes['sha256'], attributes['x_amz_sha256_tree'], (attributes['modification_time']) )
           )
  conn.commit()
  conn.close()

def lookup_file_by_path(path,dbCreds,vault):
  attributes = {}
  conn = mdb.connect(dbCreds['server'] , dbCreds['user'], dbCreds['password'], dbCreds['database'], cursorclass=MySQLdb.cursors.DictCursor);
  c = conn.cursor()
  c.execute('''SELECT * FROM {} where path = "%s" '''.format(vault) % ( path ) )
  data = c.fetchone()
  if data:
    for key in data.keys():
      attributes[key] = data[key]
  conn.close
  return attributes

def lookup_file_by_sha256(sha256,dbCreds,vault):
  matches = []
  conn = mdb.connect(dbCreds['server'] , dbCreds['user'], dbCreds['password'], dbCreds['database']);
  c = conn.cursor()
  for files in c.execute('''SELECT path, size, modification_time FROM {} where sha256 = '%s' '''.format(vault) % ( sha256 ) ):
    matches.append(files)
  conn.close
  return matches

def dup_check(dbCreds,vault):
  sha256Sums = []
  data = {}
  conn = mdb.connect(dbCreds['server'] , dbCreds['user'], dbCreds['password'], dbCreds['database']);
  c = conn.cursor()
  for sha256Sum in c.execute('''SELECT sha256 from {}}''').format(vault):
    sha256Sums.append(sha256Sum[0])
  conn.close

  collection = collections.Counter(sha256Sums)
  dupes = [i for i in collection if collection[i]>1]
  for sha256 in dupes:
    data[sha256] = lookup_file_by_sha256(sha256,dbCreds,vault)
  return data

def get_description(path,dbCreds,vault):
  attributes = {path : {}}
  conn = mdb.connect(dbCreds['server'] , dbCreds['user'], dbCreds['password'], dbCreds['database']);
  c = conn.cursor()
  c.execute('''SELECT name, path, size, UNIX_TIMESTAMP(modification_time) AS modification_time FROM {} where path = '%s' '''.format(vault) % ( path ) )
  data = c.fetchone()
  attributes[path]['name'] = data[0]
  attributes[path]['path'] = data[1]
  attributes[path]['size'] = data[2]
  attributes[path]['modification_time'] = data[3]
  conn.close
  return json.dumps(attributes)

def upload_glacier(path,vault,dbCreds):
  description = get_description(path,dbCreds,vault)

  try:
    glacier_connection = boto.connect_glacier()
    vaultObj = glacier_connection.get_vault(vault)

    start = time.time()
    archiveID = vaultObj.upload_archive(path,description)
    elapsed = time.time() - start
    transferred = os.path.getsize(path)
    Mbps = ((transferred/elapsed)*8)/(1024*1024)

  except Exception, e:
    if 'ThrottlingException' in str(e):
      response = "ThrottlingException"
      return (response)
    else:
      print("error on " + path + " " + str(e))
      archiveID = 'FAILED'
      Mbps = 0

  #we need to make a record of everything that we uploaded to ease retrieval and prevent dup backups
  conn = mdb.connect(dbCreds['server'] , dbCreds['user'], dbCreds['password'], dbCreds['database']);
  c = conn.cursor()
  start = time.time()
  c.execute('''BEGIN''')
  c.execute('''UPDATE {} SET uploaded = '%s', uploaded_at = FROM_UNIXTIME(%s), x_amz_archive_id = '%s', vault = '%s' where path = '%s' '''.format(vault) % ( "TRUE", str(time.time()), archiveID, vault, path ) )
  elapsed = time.time() - start
  conn.commit()
  conn.close

  return (archiveID,Mbps)

def upload_glacier_list(queue,vault,dbCreds,verbosity):
  while True:
    if queue.empty() == True:
      return 
    fileName = queue.get()
    try:
      response = upload_glacier(fileName,vault,dbCreds)
      while str(response) == "ThrottlingException":
        if verbosity: print "throttled, waiting"
        response = upload_glacier(fileName,vault,dbCreds)
        time.sleep(random.random() + 1)
      if verbosity: print fileName + " upload finished at " + str(response[1]) + " Mbps with archiveID: " + response[0]
    except Exception, e:
      print("error on " + fileName + " " + str(e))

def upload_s3(path,bucket,dbCreds):
  archiveID = path
  description = get_description(path,dbCreds,bucket)
  descDict = json.loads(description)
  metadata = descDict[path]

  #brief sleep to stagger thread connects and cut down on throttle messages
  time.sleep(random.random())
  try:
    s3_connection = boto.connect_s3()
    bucketObj = s3_connection.get_bucket(bucket)

    start = time.time()
    size = os.path.getsize(path)
    uploadObj = bucketObj.initiate_multipart_upload(path,metadata=metadata)
    chunkSize = 52428800
    chunkCount = int(math.ceil(size / chunkSize))

    for i in range(chunkCount + 1):
      offset = chunkSize * i
      bytes = min(chunkSize, size - offset)
      with FileChunkIO(path, 'r', offset=offset, bytes=bytes) as fp:
        uploadObj.upload_part_from_file(fp, part_num=i + 1)

    response = uploadObj.complete_upload()

    elapsed = time.time() - start
    Mbps = ((size/elapsed)*8)/(1024*1024)

  except Exception, e:
    if 'ThrottlingException' in str(e):
      response = "ThrottlingException"
      return (response)
    else:
      print("error on " + path + " " + str(e))
      archiveID = 'FAILED'
      Mbps = 0

  #we need to make a record of everything that we uploaded to ease retrieval and prevent dup backups
  conn = mdb.connect(dbCreds['server'] , dbCreds['user'], dbCreds['password'], dbCreds['database']);
  c = conn.cursor()
  start = time.time()
  c.execute('''BEGIN''')
  c.execute('''UPDATE {} SET uploaded = '%s', uploaded_at = FROM_UNIXTIME(%s), x_amz_archive_id = '%s', vault = '%s' where path = '%s' '''.format(bucket) % ( "TRUE", str(time.time()), archiveID, bucket, path ) )
  elapsed = time.time() - start
  conn.commit()
  conn.close

  return (archiveID,Mbps)

def upload_s3_list(queue,bucket,dbCreds,verbosity):
  while True:
    if queue.empty() == True:
      return 
    fileName = queue.get()
    try:
      response = upload_s3(fileName,bucket,dbCreds)
      while str(response) == "ThrottlingException":
        if verbosity: print "throttled, waiting"
        response = upload_s3(fileName,bucket,dbCreds)
        time.sleep(random.random() + 1)
      if verbosity: print fileName + " upload finished at " + str(response[1]) + " Mbps with archiveID: " + response[0]
    except Exception, e:
      print("error on " + fileName + " " + str(e))

#for each file in the directories, check if in db if not get attributes and add to db
def get_backup_list(queue,dbCreds,verbosity,vault):
  backupFiles = []

  while True:
    if queue.empty() == True:
      return 
    fileName = queue.get()
    try:
      fileInfo = lookup_file_by_path(fileName,dbCreds,vault)
      if not fileInfo:
        start = time.time()
        attributes = get_attributes(fileName,verbosity)
        end = time.time() - start
        if verbosity: print("finished in ") + str(end) + (" seconds")
        insert_file(attributes,dbCreds,vault)
        fileInfo = lookup_file_by_path(fileName,dbCreds,vault)

    except Exception, e:
      print("error on " + fileName + " " + str(e))

def check_change(path,dbCreds,vault):
  changed = False
  try:
    dbFileInfo = lookup_file_by_path(path,dbCreds,vault)
    osFileSize = os.path.getsize(path)
    if dbFileInfo['size'] != osFileSize and dbFileInfo['uploaded']:
      changed = True

  except Exception, e:
    print("error on " + files + " " + str(e))

  return changed

def get_changed_list(queue,fileNames,dbCreds,verbosity,vault,changedFiles):
  while True:
    if queue.empty() == True:
      return 
    fileName = queue.get()
    try:
      if check_change(fileName,dbCreds,vault):
        changedFiles.append(fileName)

    except Exception, e:
      print("error on " + files + " " + str(e))

  return changedFiles

def delete_backup(path,vault,dbCreds,verbosity):

  conn = mdb.connect(dbCreds['server'] , dbCreds['user'], dbCreds['password'], dbCreds['database']);
  c = conn.cursor()
  c.execute('''select x_amz_archive_id from {} where path = '%s' '''.format(vault) % ( path ) )
  archiveID = c.fetchone()

  try:
    glacier_connection = boto.connect_glacier()
    vaultObj = glacier_connection.get_vault(vault)
    response = vaultObj.delete_archive(archiveID)

  except Exception, e:
    print("error on " + path + " " + str(e))
    response = str(e)

  update_changed(path,dbCreds,verbosity,vault)

  conn.close
  return (response)

def delete_backup_list(queue,vault,dbCreds,verbosity):
  while True:
    if queue.empty() == True:
      return 
    fileName = queue.get()
    try:
      delete_backup(fileName,vault,dbCreds,verbosity)
      if verbosity: print ("deleted file " + path + " from db")
    except Exception, e:
      print("error on " + fileName + " " + str(e))

def update_changed(path,dbCreds,verbosity,vault):
  attributes = get_attributes(path,verbosity)
  conn = mdb.connect(dbCreds['server'] , dbCreds['user'], dbCreds['password'], dbCreds['database']);
  c = conn.cursor()
  c.execute('''BEGIN''')
  c.execute('''UPDATE {} SET uploaded = NULL, uploaded_at = NULL, x_amz_archive_id = NULL, vault = NULL where path = '%s' '''.format(vault) % ( path ) )
  c.execute('''UPDATE {} SET size = '%s', sha256 = '%s', x_amz_sha256_tree = '%s', modification_time = FROM_UNIXTIME(%s) where path = '%s' '''.format(vault) % ( attributes['size'] , attributes['sha256'], attributes['x_amz_sha256_tree'], attributes['modification_time'], path ))
  conn.commit()
  conn.close

def split_seq(seq, size):
  random.shuffle(seq)
  newseq = []
  splitsize = 1.0/size*len(seq)
  for i in range(size):
    newseq.append(seq[int(round(i*splitsize)):int(round((i+1)*splitsize))])
  return newseq

