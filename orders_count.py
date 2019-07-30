#!/usr/bin/python

import sys
import os
from influxdb import InfluxDBClient
from datetime import datetime
import pytz

influxdb_dbname = "data_1C"
influxdb_host = "lexx.tdz.ru"
influxdb_port = 8086
influxdb_user = ""
influxdb_passwd = ""

filename = "/mnt/sphere/var/log/orders.log"
fstat = os.stat(filename)
content = ["", ""]
data = []

with open(filename, "r") as f:
	for li in f:
		content[0] = content[1]
		content[1] = li
f.close()

for x in content:
	data.append(x.split('\t'))

for x in data:
	x[1] = x[1].rstrip().split(', ')	

data[0][1].sort()
data[1][1].sort()

len0 = len(data[0][1]) 
len1 = len(data[1][1]) 

#print data[0][1]
#print data[1][1]
dublicates = 0
for i in range(min(len0, len1)):
	if data[0][1][i] == data[1][1][i]:
		dublicates += 1
	else:
		break

#print str(dublicates) + " duplicate(s)"
#for j in range(i, len(data[1][1])):
#	print data[1][0] + " " + data[1][1][j]	

local_tz = pytz.timezone("Europe/Moscow")
date_time = datetime.strptime(data[1][0], "%d.%m.%Y %H:%M:%S")
local_dt = local_tz.localize(date_time, is_dst=None)
utc_dt = local_dt.astimezone(pytz.utc)
date_time = utc_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
#print date_time

orders_count = len(data[1][1]) - dublicates
#print str(len(data[1][1])) + " - " + str(dublicates)
json_body = [
	{
		"measurement": "orders_count",
		"tags": {
			"host_1C": "sphere",
			"database_1C": "torg"
		},
		"time": date_time,
		"fields": {
			"dublicates": dublicates,
			"orders_count": orders_count
		}
	}
]

#print repr(json_body)

client = InfluxDBClient(influxdb_host, influxdb_port,influxdb_user,influxdb_passwd,influxdb_dbname)
client.write_points(json_body)
