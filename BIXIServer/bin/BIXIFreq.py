# Spark Program for aggregating bus frequency
# RUNTIME: run as per user request
# USE: TTCFreq.py <startTimeIn> <endTimeIn> <routeTag> <fileName>

# LOGIC
# 1. Query data in time period specified by <startTime> <endTime>
# 2. Read in start lat and lon for specified route
# 3. Calculate lat and lon range at precision level <Prec> within tolerance <Tol>
# 4. Loop through specificied time period at <timeInt>
#   4.1 Select <timeInt> of data for route within lat and lon range
#   4.2 Verify time btw record is more than <minSpace>
#   4.4 Count number of buses in interval
# 5. Convert result to JSON str and save to <fileName>

from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
import sys
import datetime as DT
from datetime import timedelta
from os import environ

# Parameters
Prec = 3
Tol = 0.002
timeInt = timedelta(hours = 1)
minSpace = timedelta(minutes = 2)

#print sys.argv[1] + ' ' + sys.argv[2]

# Initialize Variables
startTime = DT.datetime.strptime(sys.argv[1], "%Y-%m-%d %H:%M:%S")
endTime = DT.datetime.strptime(sys.argv[2], "%Y-%m-%d %H:%M:%S")
station_id = sys.argv[3]
fileName = sys.argv[4]
driverName = environ["jdbcDriverName"]
dbSrcUrl = environ["mysql_srcUrl"]
dbDestUrl = environ["mysql_destUrl"]

# Start Spark SQL Context
sc = SparkContext("local", "TTCData")
sqlContext = SQLContext(sc)

# Query lat and lon data for specified time period and routeTag
initQuery = "(SELECT dateTime, nbBikes, lat, lon FROM BIXI WHERE dateTime>='" + str(startTime) + "' AND dateTime<'" + str(endTime) + \
                "' AND station_id=" + str(station_id) + ") AS T"
routeTable = sqlContext.load(None, "jdbc", None, url=dbSrcUrl, dbtable=initQuery, driver=driverName)
sqlContext.registerDataFrameAsTable(routeTable, "rawData")

# Query route stop lat and lon
initQuery = "(SELECT start_lat, start_lon FROM BIXI_ROUTES WHERE route_short_name=" + str(routeTag) + ") AS T"
routeLoc = sqlContext.load(None, "jdbc", None, url=dbDestUrl, dbtable=initQuery, driver=driverName).collect()

# Calculate lat and lon ranges
startLatUpper = round(float(str(routeLoc[0].start_lat)), Prec) + Tol
startLatLower = round(float(str(routeLoc[0].start_lat)), Prec) - Tol
startLonUpper = round(float(str(routeLoc[0].start_lon)), Prec) + Tol
startLonLower = round(float(str(routeLoc[0].start_lon)), Prec) - Tol

# Loop through at time interval
curTime = startTime
freqList = []
freqCount = {}
freq = 0

while curTime < endTime:
    #print "Count: " + str(endTableSize)
    tempTable = sqlContext.sql("SELECT * FROM rawData WHERE dateTime>='" + str(curTime) + "' AND dateTime<'" + str(curTime + timeInt) + "'")
    sqlContext.registerDataFrameAsTable(tempTable, "results")
    startTable = sqlContext.sql("SELECT nbBikes, dateTime FROM results WHERE lat>=" + str(startLatLower) + \
                                   " AND lat<=" + str(startLatUpper) + " AND lon>=" + str(startLonLower) + \
                                   " AND lon<=" + str(startLonUpper) + " ORDER BY vehicle_id ASC, dateTime ASC").collect()

    freq = 0
    for i in range (1, len(startTable)):
        curID = startTable[i][0]
        prevID = startTable[i-1][0]
        tempCur = startTable[i][1]
        tempPrev = startTable[i-1][1]
        if curID != prevID:
            freq += 1
        elif (curID == prevID) and ((tempCur - tempPrev) > minSpace):
            freq += 1
        else:
            continue

    freqCount["startTime"] = str(curTime)
    freqCount["freq"] = len(startTable)
    freqList.append(freqCount.copy())
    
    curTime += timeInt

# Stop Spark
sc.stop()

# Construct output JSON string
#[{"startTime":<"HH:MM - HH:MM">, "freq":<int>}]
jsonStr = '['
i = 0
for r in freqList:
    tempTime = DT.datetime.strptime(r["startTime"], "%Y-%m-%d %H:%M:%S")
    tempStr = tempTime.strftime("%H:%M") + " - " + (tempTime + timeInt).strftime("%H:%M")
    if i != 0 : jsonStr += ', '
    jsonStr += '{"startTime":"' + tempStr + '", "freq":' + str(r["freq"]) + '}'
    i += 1

jsonStr += ']'
#print jsonStr

f = open(fileName,'w')
f.write(jsonStr)
f.close()