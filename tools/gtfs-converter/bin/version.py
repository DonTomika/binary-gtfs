#!/usr/bin/python
# -*- coding: utf8 -*- 

import sys
import os
import sqlite3
import httplib
import time
import xml.etree.ElementTree as ET

con_db = None

try:
	con_db = sqlite3.connect(sys.argv[1])
	
	cur = con_db.cursor()
	cur.execute('UPDATE metadata SET content_version = %d' % int(sys.argv[2]))
	
	con_db.commit()
	
finally:
	if con_db:
		con_db.close()
