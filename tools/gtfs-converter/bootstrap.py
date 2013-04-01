#!/usr/bin/python
# -*- coding: utf8 -*- 

import sys
import os
import os.path
import shutil
import urllib2
import zipfile
import subprocess

# create global directory structure
if not os.path.exists("bin"):
	os.mkdir("bin")
	
	print "bin/ must contain the following executables:"
	print "  db-generator        "
	print "  gtfs-processor.jar  "
	print "  gzip                "
	print "  geodata.py          "
	print "  sqlite3             "
	print "  version.py          "
	print ""
	print "check the manual for more information"

if not os.path.exists("feeds"):
	os.mkdir("feeds")

if not os.path.exists("shared"):
	os.mkdir("shared")

if not os.path.exists("tmp"):
	os.mkdir("temp")

if len(sys.argv) < 2:
	feed_name = raw_input('feed name to process: ')
else:
	feed_name = sys.argv[1]

# create feed directory structure
feed_path = "feeds/" + feed_name + "/"

if not os.path.exists(feed_path + "gtfs"):
	os.mkdir(feed_path + "gtfs")

if not os.path.exists(feed_path + "intermediate"):
	os.mkdir(feed_path + "intermediate")

if not os.path.exists(feed_path + "output"):
	os.mkdir(feed_path + "output")

# load configuration
feed_url = ""

if os.path.exists(feed_path + "config.py"):
	with open(feed_path + "config.py") as f:
		code = compile(f.read(), feed_path + "config.py", 'exec')
		exec(code)
	
	if feed_url == "":
		print "feed_url is not set in " + feed_path + "config.py"
		print "please fix or delete the file and try again"
		sys.exit(0)
else:
	feed_url= raw_input('feed url: ')
	
	with open(feed_path + "config.py", 'w') as f:
		f.write('feed_url = "' + feed_url + '"' + "\n")

feed_version = 0

if os.path.exists(feed_path + "version.py"):
	with open(feed_path + "version.py") as f:
		code = compile(f.read(), feed_path + "version.py", 'exec')
		exec(code)
	
	if feed_version == "":
		print "feed_version is not set in " + feed_path + "version.py"
		print "please fix or delete the file and try again"
		sys.exit(0)

feed_version = feed_version + 1

temp = raw_input('feed version [' + ("%d" % feed_version) + ']: ')
if temp != "":
	feed_version = int(temp)

with open(feed_path + "version.py", 'w') as f:
	f.write('feed_version = ' + ("%d" % feed_version) + "\n")

# download feed
print "downloading gtfs feed..."

feed_zip_file = urllib2.urlopen(feed_url)
output = open("tmp/gtfs.zip", 'wb')
output.write(feed_zip_file.read())
output.close()

feed_filename = ""

# unzip files
print "extracting gtfs feed..."

zfile = zipfile.ZipFile("tmp/gtfs.zip")
for f in zfile.infolist():
	name, date_time = f.filename, f.date_time
	
	if feed_filename == "":
		counter = 0
		
		while True:
			counter = counter + 1
			feed_filename = feed_name + "-" + ("%04d" % date_time[0]) + ("%02d" % date_time[1]) + ("%02d" % date_time[2]) + "-" + ("%02d" % counter)
			if not os.path.exists(feed_path + "gtfs/" + feed_filename + "/"):
				break
		
		os.mkdir(feed_path + "gtfs/" + feed_filename + "/")
	
	(dirname, filename) = os.path.split(name)
	fd = open(feed_path + "gtfs/" + feed_filename + "/" + filename, "wb")
	fd.write(zfile.read(name))
	fd.close()
zfile.close()

os.rename("tmp/gtfs.zip", feed_path + "gtfs/" + feed_filename + ".zip")

# convert gtfs to intermediate format
print "converting feed to intermediate format..."

os.mkdir(feed_path + "intermediate/" + feed_filename + "/")
subprocess.call(['java', '-Xms256m', '-Xmx1024m', '-jar', 'bin/gtfs-processor.jar', feed_path + "gtfs/" + feed_filename + "/", feed_path + "intermediate/" + feed_filename + "/intermediate.sql", feed_path + "intermediate/" + feed_filename + "/gtfs-converter.log"])

# import the result to a new sqlite db
print "importing result to a sqlite db..."

p = subprocess.Popen(['bin/sqlite3', feed_path + "intermediate/" + feed_filename + "/intermediate.db"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
stdout, stderr = p.communicate("BEGIN TRANSACTION;\n" + ".read " + '"' + feed_path + "intermediate/" + feed_filename + "/intermediate.sql" + '"' + "\n" + "COMMIT;\n" + ".exit\n")

# geodata...
print "adding geodata..."
subprocess.call(['python', 'bin/geodata.py', feed_path + "intermediate/" + feed_filename + "/intermediate.db", "shared/geodata.db"])

# metadata
print "performing final adjustments..."
subprocess.call(['python', 'bin/version.py', feed_path + "intermediate/" + feed_filename + "/intermediate.db", "%d" % feed_version])

# convert to binary
print "converting to binary format..."

os.mkdir(feed_path + "output/" + feed_filename + "/")
subprocess.call(['bin/db-generator', feed_path + "intermediate/" + feed_filename + "/intermediate.db", feed_path + "output/" + feed_filename + "/" + feed_filename + ".db"])

# gzip
shutil.copyfile(feed_path + "output/" + feed_filename + "/" + feed_filename + ".db", feed_path + "output/" + feed_filename + "/" + feed_filename + ".db-temp")
subprocess.call(['bin/gzip', '-9', feed_path + "output/" + feed_filename + "/" + feed_filename + ".db"])
os.rename(feed_path + "output/" + feed_filename + "/" + feed_filename + ".db-temp", feed_path + "output/" + feed_filename + "/" + feed_filename + ".db")
