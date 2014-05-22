from distutils.core import setup
import py2exe
import os
import platform
from backup_functions import *
import uuid
import time
import hashlib
import argparse
import errno
import sqlite3
import time
import collections
from scandir import *

setup(console=['backup.py'])