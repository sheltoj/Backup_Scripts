import os
import uuid
import time
import hashlib

def dir_list(path):
  fileNames = []
  start = time.time()
  for root, dirs, files in os.walk(path):
    if files:
      for item in files:
        fileNames.append(root + "\\" + item)
  end = time.time()
  return fileNames

def file_md5(fileName)
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
  attributes['md5'] = hashlib.md5(open(fileNames[0],'rb').read()).hexdigest()
  return attributes	


for i in range(0, 100):
  try:
    test = get_attributes(fileNames[i])
  print(test)
    except:
  print("error on " + fileNames[i])
