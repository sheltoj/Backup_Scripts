#!/usr/bin/env python
import os
import time
import argparse
import errno
import sqlite3
import time
import collections
import platform
import boto
import boto.glacier.utils
import json
from scandir import *

def get_input():
  parser = argparse.ArgumentParser(description='Glacier backup and dupcheck script')
  parser.add_argument(
                      '-d',
                      action='store',
                      dest='path',
                      help='Directory to dupcheck'
                      )
  parser.add_argument(
                      '-s',
                      action='store',
                      dest='dbFile',
                      default='files.db',
                      help='Use file other than files.db'
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
                      help='backup data, options are currently just glacier'
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

def get_attributes(fileName):
  print("getting attributes for " + fileName)
  attributes = {}
  attributes['path'] = fileName
  attributes['name'] = os.path.basename(fileName)
  attributes['size'] = os.path.getsize(fileName)

  fileObj = open(fileName,'rb')
  hashes = boto.glacier.utils.compute_hashes_from_fileobj(fileObj)
  fileObj.close()
  attributes['sha256'] = hashes[0]
  attributes['x-amz-sha256-tree'] = hashes[1]
  attributes['modification_time'] = os.stat(fileName).st_mtime
  return attributes	

def init_database(dbFile):
  conn = sqlite3.connect(dbFile)
  c = conn.cursor()
  c.execute('''CREATE TABLE files (
                                   "path" TEXT UNIQUE NOT NULL,
                                   "name" TEXT,
                                   "size" INTEGER,
                                   "sha256" TEXT,
                                   "x-amz-sha256-tree" TEXT,
                                   "modification_time" TEXT,
                                   "uploaded" TEXT,
                                   "uploaded_at" TEXT,
                                   "x-amz-archive-id" TEXT,
                                   "vault" TEXT
                                  )
            ''')
  conn.commit
  conn.close

def insert_file(attributes,dbFile):
  conn = sqlite3.connect(dbFile)
  c = conn.cursor()
  c.execute('''BEGIN''')
  c.execute('''INSERT INTO files ("path", "name", "size", "sha256", "x-amz-sha256-tree", "modification_time")
           VALUES (?,?,?,?,?,?)''', 
           ( attributes['path'], attributes['name'], attributes['size'] , attributes['sha256'], attributes['x-amz-sha256-tree'], attributes['modification_time'] )
           )
  conn.commit()
  conn.close()

def lookup_file_by_path(path,dbFile):
  attributes = {}
  conn = sqlite3.connect(dbFile)
  conn.row_factory = sqlite3.Row
  c = conn.cursor()
  c.execute('''SELECT * FROM files where path = ?''', ( [path] ) )
  data = c.fetchone()
  if data:
    for key in data.keys():
      attributes[key] = data[key]
  return attributes
  conn.close

def lookup_file_by_sha256(sha256,dbFile):
  matches = []
  conn = sqlite3.connect(dbFile)
  c = conn.cursor()
  for files in c.execute('''SELECT path, size, modification_time FROM files where sha256 = ?''', ( [sha256] ) ):
    matches.append(files)
  return matches
  conn.close

def dup_check(dbFile):
  sha256Sums = []
  data = {}
  conn = sqlite3.connect(dbFile)
  c = conn.cursor()
  for sha256Sum in c.execute('''SELECT sha256 from files'''):
    sha256Sums.append(sha256Sum[0])
  conn.close

  collection = collections.Counter(sha256Sums)
  dupes = [i for i in collection if collection[i]>1]
  for sha256 in dupes:
    data[sha256] = lookup_file_by_sha256(sha256,dbFile)
  return data

def get_description(path,dbFile):
  attributes = {path : {}}
  conn = sqlite3.connect(dbFile)
  c = conn.cursor()
  c.execute('''SELECT name, path, size, modification_time FROM files where path = ?''', ( [path] ) )
  data = c.fetchone()
  attributes[path]['name'] = data[0]
  attributes[path]['path'] = data[1]
  attributes[path]['size'] = data[2]
  attributes[path]['modification_time'] = data[3]
  return json.dumps(attributes)
  conn.close

def upload_glacier(path,vault,dbFile):
  description = get_description(path,dbFile)
  glacier_connection = boto.connect_glacier()
  vaultObj = glacier_connection.get_vault(vault)

  start = time.time()
  archiveID = vaultObj.upload_archive(path,description)
  elapsed = time.time() - start
  transferred = os.path.getsize(path)
  Mbps = ((transferred/elapsed)*8)/(1024*1024)

  #we need to make a record of everything that we uploaded to ease retrieval and prevent dup backups
  conn = sqlite3.connect(dbFile)
  c = conn.cursor()
  c.execute('''UPDATE files SET uploaded = ?, uploaded_at = ?, 'x-amz-archive-id' = ?, vault = ? where path = ?''', ( "TRUE", str(time.time()), archiveID, vault, path ) )
  conn.commit()
  conn.close

  return (archiveID,Mbps)

#for each file in the directories, check if in db if not get attributes and add to db
def get_backup_list(fileNames,dbFile,verbosity):
  backupFiles = {}
  for i, files in enumerate(fileNames):
    try:
      fileInfo = lookup_file_by_path(files,dbFile)
      if not fileInfo:
        start = time.time()
        attributes = get_attributes(files)
        end = time.time() - start
        if verbosity: print("finished in ") + str(end) + (" seconds")
        insert_file(attributes,dbFile)
        fileInfo = lookup_file_by_path(files,dbFile)

      if not fileInfo['uploaded']:
        backupFiles[fileInfo['path']] = fileInfo

    except Exception, e:
      print("error on " + files + " " + str(e))
  
  return backupFiles

