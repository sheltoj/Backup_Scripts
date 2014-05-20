#!/usr/bin/env python
import os
import uuid
import time
import hashlib
import argparse
import errno
import sqlite3
import time

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
  results = parser.parse_args()
  if not (results.path):
    parser.error('no path provided')
  return results

def dir_list(path):
  fileNames = []
  for root, dirs, files in os.walk(path):
    if files:
      for item in files:
        fileNames.append(root + "/" + item)
  return fileNames

def file_md5(fileName):
  fileHandle = open(fileName,'rb')
  fileContents = fileHandle.read()
  fileHash = hashlib.md5(open(fileName).read()).hexdigest()
  return fileHash

def get_attributes(fileName):
  attributes = {}
  attributes['path'] = fileName
  attributes['name'] = os.path.basename(fileName)
  attributes['size'] = os.path.getsize(fileName)
  attributes['uuid'] = uuid.uuid4()
  attributes['md5'] = hashlib.md5(open(fileName,'rb').read()).hexdigest()
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

def insert_host(attributes,dbFile):
  conn = sqlite3.connect(dbFile)
  c = conn.cursor()
  print (type(attributes['name']))
  c.execute('''INSERT INTO files (path, name) VALUES (?,?)''', ( attributes['path'], attributes['name'] ) )

  conn.commit()
  conn.close()









