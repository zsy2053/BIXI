# Daemon for aggregating bus frequency data for one week

from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
import sys
import datetime as DT
from datetime import timedelta
from os import environ
import MySQLdb

# Environment Variables
import ev

# Import Custom Functions
import BIXISQL

# Debug
#print 'Number of arguments:', len(sys.argv), 'arguments.'
#print 'Argument List:', str(sys.argv)
timer = DT.datetime.now()

# TTCFreqWk.py <routeTag> <endTime>

# Logic
# 1. Loop through specific time period
# 2. Read in input time interval from ttc raw data table
# 3. Read in TTC_ROUTES table
# 4. Loop through TTC_ROUTES table for all routes
#   4.1 Filter 1-hr of data
#   4.3 Count unique records in bus_id within time span
#   4.4 Save count result to TTC_FREQ table

# Constants
tableName = "bixi"
Prec = 3
Tol = 0.002

# Initialize Variables
routeTag = sys.argv[1]
endTime = DT.datetime.strptime(sys.argv[2], "%Y-%m-%d %H:%M:%S")
day = timedelta(hours = 24)
timeInt = timedelta(minutes = 15)
readInt = timedelta(hours = 6)
routeQuery = TTCSQL.getRouteSQL()
urlSrc = environ["mysql_srcUrl"]
urlDest = environ["mysql_destUrl"]
driverName = environ["jdbcDriverName"]
#driverPath = TTCSQL.getDriverPath()
username = environ["mysqldb_user"]
password = environ["mysqldb_pswd"]
dbHost = environ["mysql_host"]
dbSrc = environ["mysql_dbSrc"]
dbDest = environ["mysql_dbDest"]

# Connect to CVST_TTC database for writing
conn = MySQLdb.connect(user=username, passwd=password,db=dbDest,host=dbHost)
#conn = MySQLdb.connect(user="cvst", passwd="cvst",db="CVST_TTC",host="142.150.77.152")
curs = conn.cursor()


# Counts number of buses in specified route for 24-hr period
def countBus(station_id, endTimeIn):
    # Construct SQL Query for ttc raw data table
    initQuery = "(SELECT nbBikes, dateTime, lat, lon FROM BIXI WHERE " +\
		        "station_id = " + str(station_id) + " " +\
 		        "AND dateTime >= '" + str(endTimeIn - day) + "' AND dateTime < '" + str(endTimeIn) + "' " +\
    		    "ORDER BY nbBikes, dateTime ASC) AS T" 

    # Get tables from SQL Database
    ttcRawTable = sqlContext.load(None, "jdbc", None, url=urlSrc, dbtable=initQuery, driver=driverName)
    sqlContext.registerDataFrameAsTable(ttcRawTable, "rawData")

    # Filter data with matching location
    resTable = sqlContext.sql("SELECT nbBikes, dateTime FROM rawData WHERE " +\
                            "lat>=" + str(latLower) + " AND lat<=" + str(latUpper) +\
                            " AND lon>=" + str(lonLower) + " AND lon<=" + str(lonUpper))
    sqlContext.registerDataFrameAsTable(resTable, "resData")

    # Select unique buses in matched data
    idList = sqlContext.sql("SELECT nbBikes FROM resData").collect()

    # Find freq of each unique bus
    freq = 0
    for idRow in idList:
        finList = sqlContext.sql("SELECT dateTime FROM resData WHERE station_id=" + str(idRow.station_id) + " ORDER BY dateTime ASC").collect()
        for i in range(0, len(finList) - 1):
            if (finList[i + 1].dateTime - finList[i].dateTime) > timeInt:
                freq += 1
        freq += 1
    print freq

    return

# ---------- Main Function ---------------

# Start Spark SQL Context
sc = SparkContext("local", "BIXIData")
sqlContext = SQLContext(sc)

# Get bus route location
routeQuery = "(SELECT start_lat, start_lon, end_lat, end_lon FROM BIXI_ROUTES WHERE " +\
            "route_short_name = " + str(routeTag) + ") AS T"

routeTable = sqlContext.load(None, "jdbc", None, url=urlDest, dbtable=routeQuery, driver=driverName)
route = routeTable.collect()[0]

# Get the upper and lower bounds for the start location of the route
trgLat = route.start_lat
trgLon = route.start_lon
latUpper = round(trgLat, Prec) + Tol
latLower = round(trgLat, Prec) - Tol
lonUpper = round(trgLon, Prec) + Tol
lonLower = round(trgLon, Prec) - Tol

# Scale factor for route with same start and end location
rpFactor = 1
if (route.end_lat >= latLower) and (route.end_lat <= latUpper) and (route.end_lon >= lonLower) and (route.end_lon <= lonUpper):
    rpFactor = 2
else:
    rpFactor = 1


# Loop through time range in <readInt> intervals
startTime = DT.datetime.strftime(endTime - 7 * day, "%Y-%m-%d") + " 00:00:00"
endInt = DT.datetime.strptime(startTime, "%Y-%m-%d %H:%M:%S") + day

# Compute bus freq per day over a week
while endInt <= endTime:
    print str(endInt - day) + ' - ' + str(endInt)
    countBus(routeTag, endInt)
    # Increment read interval
    endInt += day
    #conn.commit()

# Stop Spark
sc.stop()

# Close db connection
conn.close()

# Debug
print DT.datetime.now() - timer
