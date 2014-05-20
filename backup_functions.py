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
                      dest='dbfile',
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
                                   "path" text,
                                   "name" text,
                                   "size" integer,
                                   "md5" text,
                                   "x-amz-sha256" text,
                                   "modification_time" text,
                                   "uploaded" integer,
                                   "uploaded_at" text
                                  )
            ''')
  conn.commit
  conn.close














