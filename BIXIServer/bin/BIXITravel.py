# Program for aggregating travel time data
# RUNTIME: run as per user request
# USE: TTCTravel.py <startTime> <endTime> <routeTag> <fileName>
# CONTRAINT: endTimeIn - startTimeIn < 12 hours

# LOGIC
# 1. Read data in period specified by <startTime> <endTime> and route <routeTag>
# 2. Loop through time period at interval <timeInt>
#   2.1 Sample up to <maxSampleSize> number of unique vehicle IDs in interval
# 3. Eliminate duplicate entries in ID list
# 4. Loop through list of vehicle IDs
#   4.1 Compute travel times during each time interval
# 5. Calculate average travel time per time interval
# 6. Convert result to JSON and save to <fileName>

from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql import Row
from pyspark.sql.functions import *
from pyspark.sql import functions as F
import sys
import datetime as DT
from datetime import timedelta
from os import environ

# Parameters
maxSampleSize = 3
maxInt = timedelta(hours = 12)
timeInt = timedelta(hours = 1)
minTravel = 8

# Initialize Variables
startTime = DT.datetime.strptime(sys.argv[1], "%Y-%m-%d %H:%M:%S")
endTime = DT.datetime.strptime(sys.argv[2], "%Y-%m-%d %H:%M:%S")
routeTag = sys.argv[3]
fileName = sys.argv[4]
driverName = environ["jdbcDriverName"]
dbSrcUrl = environ["mysql_srcUrl"]

# Verify Input (max 12-hr interval)
#if (endTime - startTime) > maxInt:
#    endTime = startTime + maxInt

# Get route info
# Construct SQL Query for ttc raw data table
initQuery = "(SELECT * FROM BIXI WHERE dateTime >= '" + str(startTime) + "' " + \
            "AND dateTime < '" + str(endTime) + "' AND station_id = " + str(station_id) + ") AS T"
#print "Initial Query: " + initQuery

# Start Spark SQL Context
sc = SparkContext("local", "BIXIData")
sqlContext = SQLContext(sc)

# Get tables from SQL Database
bixiRawTable = sqlContext.load(None, "jdbc", None, url=dbSrcUrl, dbtable=initQuery, driver=driverName)
sqlContext.registerDataFrameAsTable(bixiRawTable, "rawData")
if ttcRawTable.count() < 1:
    sc.stop()
    print "[BIXITravel.py] ERROR: Query returned empty table, no matching records"
    sys.exit()


# Select a sample list of bus ids (one id per interval)
idList = sqlContext.sql("SELECT DISTINCT(nbBikes) FROM rawData WHERE dateTime>='" + str(startTime) + "' AND dateTime<'" + str(startTime + timeInt) + "' LIMIT " + str(maxSampleSize))
curTime = startTime + timeInt

while curTime < endTime:
    temp = sqlContext.sql("SELECT DISTINCT(nbBikes) FROM rawData WHERE dateTime>='" + str(curTime) + "' AND dateTime<'" + str(curTime + timeInt) + "' LIMIT " + str(maxSampleSize))
    idList = idList.unionAll(temp)
    #print "ID COUNT: " + str(idList.count())
    #print str(curTime)
    curTime += timeInt

#idList.show()
idList = idList.distinct().collect()
print "ID COUNT: " + str(len(idList))


# Loop through bus id list to calculate travel time
#print "Route: " + str(routeTag)
trvSchm = ['startTime', 'trvTime']
avgSchm = ['startTime', 'count', 'trvTime']
trvTimeList = sqlContext.createDataFrame([('00:00:00', 0)], trvSchm).limit(0)
trvSumList = {}
trvCountList = {}
travelList = []
travelTime = {}

# Creat empty dictionaries for sum and count data
curTime = startTime
while curTime < endTime:
    trvSumList[str(curTime)] = 0
    trvCountList[str(curTime)] = 0
    curTime += timeInt

# add travel time to result dictionaries
def addResult(srcList):
    for row in srcList:
        trvSumList[str(row[0])] += row[2]
        trvCountList[str(row[0])] += row[1]
        

# Compute travel times
for busrow in idList:
    print busrow.vehicle_id
    temp = sqlContext.sql("SELECT dateTime FROM rawData WHERE nbBikes=" + str(busrow.nbBikes)).collect()
    rangeSize = len(temp)
    print str(temp[0].dateTime) + " " + str(temp[0].dirTag)
    print "List Size: " + str(rangeSize)
    trvStart = temp[0].dateTime
    trvCount = 0
    trvSum = 0
    trvInt = str(trvStart.date()) + " " + str(trvStart.hour).zfill(2) + ":00:00"
    trvInt = DT.datetime.strptime(trvInt, "%Y-%m-%d %H:%M:%S")

    for i in range(1, rangeSize):
        if (temp[i].dirTag != temp[i-1].dirTag) or (i == (rangeSize - 1)):  # bus changed direction
            if i == (rangeSize - 1):
                trvEnd = temp[i].dateTime
            else:
                trvEnd = temp[i-1].dateTime
            tempTrip = (trvEnd - trvStart).total_seconds() / 60  # caculate travel time in minutes
            if tempTrip > minTravel:    # filter out invalid short trip
                trvSum += tempTrip
                trvCount += 1
            trvStart = temp[i].dateTime
            if ((trvStart > (trvInt + timeInt)) or (i == (rangeSize - 1))) and (trvCount != 0):
                save data and proceed to next time interval
                print str(trvInt)
                newRow = sqlContext.createDataFrame([(str(trvInt), int(trvSum / trvCount))], trvSchm)
                trvTimeList = trvTimeList.unionAll(newRow)
                trvCount = 0
                trvSum = 0
                trvInt = str(trvStart.date()) + " " + str(trvStart.hour).zfill(2) + ":00:00"
                trvInt = DT.datetime.strptime(trvInt, "%Y-%m-%d %H:%M:%S")

    temp = trvTimeList.groupBy('startTime').agg({'trvTime':'sum', 'startTime':'count'}).collect()  # group travel time by interval
    addResult(temp)

    trvTimeList = trvTimeList.limit(0)  # clear data in dataframe to prevent overflow (max 100)

#Stop Spark Context
sqlContext.clearCache()
sc.stop()

# Compute average travel time
for key in trvSumList.keys():
    tempTime = DT.datetime.strptime(key, "%Y-%m-%d %H:%M:%S")
    tempStr = tempTime.strftime("%H:%M") + " - " + (tempTime + timeInt).strftime("%H:%M")
    travelTime['startTime'] = tempStr

    if trvCountList[key] == 0:
        travelTime['travelTime'] = trvSumList[key]
    else:
        travelTime['travelTime'] = round(trvSumList[key] / trvCountList[key])
    travelList.append(travelTime.copy())

# Construct output JSON string
#[{"startTime":<"HH:MM - HH:MM">, "travelTime":<int>}]
jsonStr = '['
i = 0
for r in travelList:
    if i != 0 : jsonStr += ', '
    jsonStr += '{"startTime":"' + r["startTime"] + '", "travelTime":' + str(r["travelTime"]) + '}'
    i += 1

jsonStr += ']'
#print jsonStr

f = open(fileName,'w')
f.write(jsonStr)
f.close()

