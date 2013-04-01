#!/usr/bin/python
# -*- coding: utf8 -*- 

import sys
import os
import sqlite3
import httplib
import time
import xml.etree.ElementTree as ET

con_db = None
con_shared = None

try:
	con_db = sqlite3.connect(sys.argv[1])
	con_shared = sqlite3.connect(sys.argv[2])
	
	cur = con_db.cursor()
	
	cur.execute('CREATE INDEX s1 ON stops (latitude, longitude)')
	con_db.commit()
	
	cur.execute('SELECT DISTINCT name, latitude, longitude FROM stops')
	
	rows = cur.fetchall()
	
	cur_shared = con_shared.cursor()
	
	# fill cache with new data
	cur_shared.executemany("INSERT OR IGNORE INTO reverse_geo (name, latitude, longitude) VALUES (?, ?, ?)", rows)
	
	con_shared.commit();
	
	cur_shared.execute("SELECT id, latitude, longitude FROM reverse_geo WHERE osm_xml IS NULL ORDER BY id")
	
	places = cur_shared.fetchall()
	
	conn = httplib.HTTPConnection("open.mapquestapi.com")
	counter = 0

	for row in places:
		counter = counter + 1
		sys.stdout.write("\rprocessing place %d of %d..." % (counter, len(places)))
		
		url = "/nominatim/v1/reverse?format=xml&lat=" + ("%f" % (float(row[1]) / 1000000.0)) + "&lon=" + ("%f" % (float(row[2]) / 1000000.0)) + "&addressdetails=1"
		headers = { "User-Agent": "A simple GTFS tool written in python (dontomika@gmail.com)" }
		conn.request("GET", url, "", headers)
		r1 = conn.getresponse()
		data = r1.read()
		
		# print url
		
		root = ET.fromstring(data)
		# root = tree.getroot()
		addressparts = root.find('addressparts')
		
		if addressparts is not None:
			road = addressparts.find('road')
			district = addressparts.find('city_district')
			
			if road is not None and district is not None:
				cur_shared.execute('UPDATE reverse_geo SET osm_xml = ?, street = ?, district = ? WHERE id = ?', (data.decode('utf-8'), road.text, district.text, row[0]))
				con_shared.commit()
			elif road is not None:
				cur_shared.execute('UPDATE reverse_geo SET osm_xml = ?, street = ? WHERE id = ?', (data.decode('utf-8'), road.text, row[0]))
				con_shared.commit()
			elif district is not None:
				cur_shared.execute('UPDATE reverse_geo SET osm_xml = ?, district = ? WHERE id = ?', (data.decode('utf-8'), district.text, row[0]))
				con_shared.commit()
			else:
				cur_shared.execute('UPDATE reverse_geo SET osm_xml = ? WHERE id = ?', (data.decode('utf-8'), row[0]))
				con_shared.commit()
		
		time.sleep(0.5)
	
	con_shared.commit()
	
	# fix wrong values
	sys.stdout.write("\rfixing wrong values...                          ")
	
	cur_shared.execute("UPDATE reverse_geo SET street = NULL WHERE street + 0 = street");
	cur_shared.execute("UPDATE reverse_geo SET district = district || '. kerület' WHERE district + 0 = district")

	cur_shared.execute("UPDATE reverse_geo SET district = 'I. kerület' WHERE district = '1. kerület'")
	cur_shared.execute("UPDATE reverse_geo SET district = 'II. kerület' WHERE district = '2. kerület'")
	cur_shared.execute("UPDATE reverse_geo SET district = 'III. kerület' WHERE district = '3. kerület'")
	cur_shared.execute("UPDATE reverse_geo SET district = 'IV. kerület' WHERE district = '4. kerület'")
	cur_shared.execute("UPDATE reverse_geo SET district = 'V. kerület' WHERE district = '5. kerület'")
	cur_shared.execute("UPDATE reverse_geo SET district = 'VI. kerület' WHERE district = '6. kerület'")
	cur_shared.execute("UPDATE reverse_geo SET district = 'VII. kerület' WHERE district = '7. kerület'")
	cur_shared.execute("UPDATE reverse_geo SET district = 'VIII. kerület' WHERE district = '8. kerület'")
	cur_shared.execute("UPDATE reverse_geo SET district = 'IX. kerület' WHERE district = '9. kerület'")
	cur_shared.execute("UPDATE reverse_geo SET district = 'X. kerület' WHERE district = '10. kerület'")
	cur_shared.execute("UPDATE reverse_geo SET district = 'XI. kerület' WHERE district = '11. kerület'")
	cur_shared.execute("UPDATE reverse_geo SET district = 'XII. kerület' WHERE district = '12. kerület'")
	cur_shared.execute("UPDATE reverse_geo SET district = 'XIII. kerület' WHERE district = '13. kerület'")
	cur_shared.execute("UPDATE reverse_geo SET district = 'XIV. kerület' WHERE district = '14. kerület'")
	cur_shared.execute("UPDATE reverse_geo SET district = 'XV. kerület' WHERE district = '15. kerület'")
	cur_shared.execute("UPDATE reverse_geo SET district = 'XVI. kerület' WHERE district = '16. kerület'")
	cur_shared.execute("UPDATE reverse_geo SET district = 'XVII. kerület' WHERE district = '17. kerület'")
	cur_shared.execute("UPDATE reverse_geo SET district = 'XVIII. kerület' WHERE district = '18. kerület'")
	cur_shared.execute("UPDATE reverse_geo SET district = 'XIX. kerület' WHERE district = '19. kerület'")
	cur_shared.execute("UPDATE reverse_geo SET district = 'XX. kerület' WHERE district = '20. kerület'")
	cur_shared.execute("UPDATE reverse_geo SET district = 'XXI. kerület' WHERE district = '21. kerület'")
	cur_shared.execute("UPDATE reverse_geo SET district = 'XXII. kerület' WHERE district = '22. kerület'")
	cur_shared.execute("UPDATE reverse_geo SET district = 'XXIII. kerület' WHERE district = '23. kerület'")
	cur_shared.execute("UPDATE reverse_geo SET district = 'XXIV. kerület' WHERE district = '24. kerület'")
	cur_shared.execute("UPDATE reverse_geo SET district = 'XXV. kerület' WHERE district = '25. kerület'")
	cur_shared.execute("UPDATE reverse_geo SET district = 'XXVI. kerület' WHERE district = '26. kerület'")
	cur_shared.execute("UPDATE reverse_geo SET district = 'XXVII. kerület' WHERE district = '27. kerület'")
	cur_shared.execute("UPDATE reverse_geo SET district = 'XXVIII. kerület' WHERE district = '28. kerület'")
	cur_shared.execute("UPDATE reverse_geo SET district = 'XXIX. kerület' WHERE district = '29. kerület'")
	cur_shared.execute("UPDATE reverse_geo SET district = 'XXX. kerület' WHERE district = '30. kerület'")

	con_shared.commit()
	
	sys.stdout.write("\rconverting coordinates to place names...           ")
	
	# do the real stuff (latlon -> street lookup)
	cur.execute("CREATE TABLE geo_temp (latitude INT, longitude INT, street TEXT)")
	cur.execute("CREATE INDEX geo1 ON geo_temp (latitude, longitude)")
	
	cur_shared.execute("SELECT latitude, longitude, street, district FROM reverse_geo")
	rows = cur_shared.fetchall()
	
	stuff = []
	for row in rows:
		lat, lon, street, district = row
		
		if street is not None and district is not None:
			str = street + ", " + district
		elif street is not None:
			str = street
		elif district is not None:
			str = district
		else:
			str = ""
		
		stuff.append((lat, lon, str))
	
	cur.executemany("INSERT INTO geo_temp VALUES(?, ?, ?)", stuff)
	
	cur.execute("UPDATE stops SET street = (SELECT street FROM geo_temp WHERE geo_temp.latitude = stops.latitude AND geo_temp.longitude = stops.longitude)")
	
	cur.execute("DROP TABLE geo_temp")
	
	con_db.commit()
	
	# other fixes
	sys.stdout.write("\rother fixes...                                       ")
	
	cur.execute("UPDATE stops SET flags = flags | 4 WHERE id IN (SELECT rs.stop_id FROM route_stops rs INNER JOIN routes r ON r.id = rs.route_id WHERE r.category_id = 1)")
	
	cur.execute("UPDATE stops SET subname = 'metró' WHERE id IN (SELECT rs.stop_id FROM route_stops rs INNER JOIN routes r ON r.id = rs.route_id WHERE r.category_id = 1)")
	
	cur.execute("UPDATE stops SET subname = 'villamos' WHERE id IN (SELECT rs.stop_id FROM route_stops rs INNER JOIN routes r ON r.id = rs.route_id WHERE r.category_id = 2 AND r.color <> 'FF1818') AND subname = ''")
	
	cur.execute("UPDATE stops SET subname = 'HÉV' WHERE id IN (SELECT rs.stop_id FROM route_stops rs INNER JOIN routes r ON r.id = rs.route_id WHERE r.category_id = 6) AND subname = ''")
	
	cur.execute("UPDATE stops SET subname = 'hajó' WHERE id IN (SELECT rs.stop_id FROM route_stops rs INNER JOIN routes r ON r.id = rs.route_id WHERE r.category_id = 7) AND subname = ''")
	
	con_db.commit()
	
	sys.stdout.write("\r                                                   ")
	sys.stdout.write("\r")
	
finally:
	if con_db:
		con_db.close()
	
	if con_shared:
		con_shared.close()
