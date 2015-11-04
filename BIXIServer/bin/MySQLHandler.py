# Functions for requesting JSON data from MySQL Database or Spark

import collections
import MySQLdb
import json
import datetime
import gviz_api
import sys
import subprocess
from os import environ

# Environ vars import
import ev

# Initialize variables
sparkSubmit = environ["sparkSubmit"]
driverName = environ["jdbcDriverName"]
driverPath = environ["jdbcDriverPath"]
username = environ["mysqldb_user"]
password = environ["mysqldb_pswd"]
dbHost = environ["mysql_host"]
dbSrc = environ["mysql_dbSrc"]
dbDest = environ["mysql_dbDest"]
colMax = environ["colMax"]


# Get full name of route based on specified route number
def getRouteName(station_id):
    conn = MySQLdb.connect(user=username, passwd=password, db=dbDest, host=dbHost)
    curs = conn.cursor()

    curs.execute("SELECT station_name FROM BIXI_ROUTES WHERE station_name=%s;", (station_id))
    rows = curs.fetchall()

    conn.close()

    return str(station_id) + " " + rows[0][0]


# Get JSON data for bus frequency graph
def getFreqData(station_id, startTime, endTime):
    # TTCFreq.py <startTimeIn> <endTimeIn> <routeTag> <fileName>
    prgName = "bin/BIXIFreq.py"
    fileName = "bin/freqJSON"

    # Call and wait for program to finish
    p = subprocess.Popen([sparkSubmit, '--master', 'local[8]', prgName, '%s'%(str(startTime)), '%s'%(str(endTime)), '%s'%(str(station_id)), '%s'%(fileName)])
    p.wait()

    objects_list = json.loads(open(fileName).read())

    # Convert to json format for Google Chart
    description = {"startTime": ("string", "startTime"), "freq": ("number", "freq")}
    data_table = gviz_api.DataTable(description)
    data_table.LoadData(objects_list)
    jsonData = data_table.ToJSon(columns_order=("startTime", "freq"), order_by="startTime")

    #print jsonData
    return jsonData


# Get JSON data for cumulative bus frequency graph
def getFreqSum(startTime, endTime):
    conn = MySQLdb.connect(user=username, passwd=password, db=dbDest, host=dbHost)
    curs = conn.cursor()

    curs.execute("SELECT station_id, SUM(freq) FROM BIXI_FREQ WHERE startTime >= '%s' AND startTime < '%s' " % (startTime, endTime)  + \
                    "GROUP BY station_id ORDER BY SUM(freq) DESC LIMIT %d" % int(colMax))
    rows = curs.fetchall()

    # Convert query to objects of key-value pairs
    objects_list = []
    for row in rows:
        d = collections.OrderedDict()
        d['station_id'] = getRouteName(row[0])
        d['freq'] = int(row[1])
        objects_list.append(d)

    conn.close()

    # Convert to json format for Google Chart
    description = {"station_id": ("string", "station_id"), "freq": ("number", "freq")}
    data_table = gviz_api.DataTable(description)
    data_table.LoadData(objects_list)
    jsonData = data_table.ToJSon(columns_order=("station_id", "freq"), order_by="freq")

    return jsonData


# Get JSON data for route travel time graph
def getTrvTime(startTime, endTime, station_id):
    # TTCTravel.py <startTimeIn> <endTimeIn> <routeTag> <fileName>
    prgName = "bin/BIXITravel.py"
    fileName = "bin/trvJSON"

    # Call and wait for program to finish
    p = subprocess.Popen([sparkSubmit, '--master', 'local[8]', prgName, '%s'%(str(startTime)), '%s'%(str(endTime)), '%s'%(str(routeTag)), '%s'%(fileName)])
    p.wait()

    objects_list = json.loads(open(fileName).read())

    # Convert to json format for Google Chart
    description = {"startTime": ("string", "startTime"), "travelTime": ("number", "travelTime")}
    data_table = gviz_api.DataTable(description)
    data_table.LoadData(objects_list)
    jsonData = data_table.ToJSon(columns_order=("startTime", "travelTime"), order_by="startTime")

    #print jsonData
    return jsonData


# Get JSON data for cumulative route travel time graph
def getTrvSum(startTime, endTime):
    conn = MySQLdb.connect(user=username, passwd=password, db=dbDest, host=dbHost)
    curs = conn.cursor()

    curs.execute("SELECT station_id, AVG(travelTime) FROM BIXI_TRAVEL WHERE startTime >= '%s' " % (startTime) + \
                    "AND startTime < '%s' GROUP BY station_id ORDER BY AVG(travelTime) DESC LIMIT %d;" % (endTime, int(colMax)))
    rows = curs.fetchall()

    # Convert query to objects of key-value pairs
    objects_list = []
    for row in rows:
        d = collections.OrderedDict()
        d['station_id'] = getRouteName(row[0])
        d['travelTime'] = int(row[1])
        objects_list.append(d)

    conn.close()

    # Convert to json format for Google Chart
    description = {"station_id": ("string", "station_id"), "travelTime": ("number", "travelTime")}
    data_table = gviz_api.DataTable(description)
    data_table.LoadData(objects_list)
    jsonData = data_table.ToJSon(columns_order=("station_id", "travelTime"), order_by="travelTime")

    return jsonData


# Get JSON data for highest bus density map
def getBusDens(startTime, endTime):
    prgName = "bin/BIXIPoint.py"
    fileName = "bin/pointJSON"

    # Max read interval is 24-hr, truncate if exceeded
    startInt = datetime.datetime.strptime(startTime, "%Y-%m-%d %H:%M:%S")
    endInt = datetime.datetime.strptime(endTime, "%Y-%m-%d %H:%M:%S")
    maxInt = datetime.timedelta(hours = 24)
    if (endInt - startInt) > maxInt : 
        endInt = startInt + maxInt
        endTime = datetime.datetime.strftime(endInt, "%Y-%m-%d %H:%M:%S")

    # Call and wait for program to finish
    p = subprocess.Popen([sparkSubmit, '--master', 'local[8]', prgName, '%s'%(str(startTime)), '%s'%(str(endTime)), '%s'%(fileName)])
    p.wait()

    objects_list = json.loads(open(fileName).read())

    # Convert to json format for Google Chart
    description = {"lat": ("number", "lat"), "lon": ("number", "lon"), "count": ("number", "count")}
    data_table = gviz_api.DataTable(description)
    data_table.LoadData(objects_list)
    jsonData = data_table.ToJSon(columns_order=("lat", "lon", "count"), order_by="count")

    return jsonData


# Get JSON data for bus route map
def getBusLine(startTime, endTime, routeTag):
    prgName = "bin/BIXILine.py"
    fileName = "bin/lineJSON"

    # Call and wait for program to finish
    p = subprocess.Popen([sparkSubmit, '--master', 'local[8]', prgName, '%s'%(str(startTime)), '%s'%(str(endTime)), '%s'%(str(routeTag)), '%s'%(fileName)])
    p.wait()

    objects_list = json.loads(open(fileName).read())

    # Convert to json format for Google Chart
    description = {"lat": ("number", "lat"), "lon": ("number", "lon"), "dateTime": ("string", "dateTime")}
    data_table = gviz_api.DataTable(description)
    data_table.LoadData(objects_list)
    jsonData = data_table.ToJSon(columns_order=("lat", "lon", "dateTime"))

    return jsonData


# Get JSON data for list of routes
def getRouteList():
    conn = MySQLdb.connect(user=username, passwd=password, db=dbDest, host=dbHost)
    curs = conn.cursor()

    curs.execute("SELECT route_short_name, route_long_name FROM BIXI_ROUTES ORDER BY route_short_name;")
    rows = curs.fetchall()

    results = []
    for row in rows:
        route = {
            'station_id' : row[0],
            'routeName' : row[1]
        }
        results.append(route.copy())

    jsonData = json.dumps(results)
    return jsonData

