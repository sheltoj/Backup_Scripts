#!/usr/bin/env python
import os
import uuid
import time
import hashlib
import argparse
import errno

def get_input():
  parser = argparse.ArgumentParser(description='Glacier backup and dupcheck script')
  parser.add_argument('-d', action='store', dest='path', help='Directory to dupcheck')
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
