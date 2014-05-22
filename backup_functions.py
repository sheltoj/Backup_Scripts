#!/usr/bin/env python
import os
import uuid
import time
import hashlib
import argparse
import errno
import sqlite3
import time
import collections
import platform
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

def file_md5(fileName, block_size=4194304):
    fileHandle = open(fileName,'rb')
    md5 = hashlib.md5()
    while True:
        data = fileHandle.read(block_size)
        if not data:
            break
        md5.update(data)
    fileHandle.close
    return md5.hexdigest()

def get_attributes(fileName):
  print("getting attributes for " + fileName)
  attributes = {}
  attributes['path'] = fileName
  attributes['name'] = os.path.basename(fileName)
  attributes['size'] = os.path.getsize(fileName)
  attributes['uuid'] = uuid.uuid4()
  attributes['md5'] = file_md5(fileName)
  attributes['modification_time'] = os.stat(fileName).st_mtime
  return attributes	

def init_database(dbfile):
  conn = sqlite3.connect(dbfile)
  c = conn.cursor()
  c.execute('''CREATE TABLE files (
                                   "path" TEXT UNIQUE NOT NULL,
                                   "name" TEXT,
                                   "size" INTEGER,
                                   "md5" TEXT,
                                   "x-amz-sha256" TEXT,
                                   "modification_time" TEXT,
                                   "uploaded" TEXT,
                                   "uploaded_at" TEXT
                                  )
            ''')
  conn.commit
  conn.close

def insert_file(attributes,dbFile):
  conn = sqlite3.connect(dbFile)
  c = conn.cursor()
  c.execute('''INSERT INTO files ("path", "name", "size", "md5", "modification_time")
           VALUES (?,?,?,?,?)''', 
           ( attributes['path'], attributes['name'], attributes['size'] , attributes['md5'], attributes['modification_time'] )
           )
  conn.commit()
  conn.close()

def lookup_file_by_path(path,dbFile):
  conn = sqlite3.connect(dbFile)
  c = conn.cursor()
  c.execute('''SELECT * FROM files where path = ?''', ( [path] ) )
  data = c.fetchone()
  return data
  conn.close

def lookup_file_by_md5(md5,dbFile):
  matches = []
  conn = sqlite3.connect(dbFile)
  c = conn.cursor()
  for files in c.execute('''SELECT path, size, modification_time FROM files where md5 = ?''', ( [md5] ) ):
    matches.append(files)
  return matches
  conn.close

def dup_check(dbFile):
  md5Sums = []
  data = {}
  conn = sqlite3.connect(dbFile)
  c = conn.cursor()
  for md5Sum in c.execute('''SELECT md5 from files'''):
    md5Sums.append(md5Sum[0])
  conn.close

  collection = collections.Counter(md5Sums)
  dupes = [i for i in collection if collection[i]>1]
  for md5 in dupes:
    data[md5] = lookup_file_by_md5(md5,dbFile)
  return data








