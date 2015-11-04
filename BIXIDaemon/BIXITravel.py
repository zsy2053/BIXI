# Daemon for aggregating travel time data and save results to TTC_TRAVEL
# TARGET: table "TTC_TRAVEL" in database "CVST_TTC"
# RUNTIME: ONCE for past data, PERIODIC for future use
# USE: TTCTravel.py <startTimeIn> <endTimeIn>

# LOGIC
# 1. Loop through specific time period at interval <readInt>
# 2. Loop through TTC_ROUTES for all routes
# 3. Read in time interval from ttc table for route
# 4. Read in stop lat, lon for route
#   4.1 Select 3-hr of data for bus at start stop
#   4.3 Find begin and end time for one <vehicle_id> within time span
#   4.4 Save duration result to TTC_TRAVEl table

from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql import Row
#import jaydebeapi
from pyspark.sql.functions import *
import sys
import datetime as DT
from datetime import timedelta
from os import environ
import MySQLdb

# Import Custom Functions
import BIXISQL

# Debug
#print 'Number of arguments:', len(sys.argv), 'arguments.'
#print 'Argument List:', str(sys.argv)

# Constants
#urlSrc = "jdbc:mysql://142.150.77.152:3306/CVST_RAW_DATA?user=lingfei&password=lingfei@112358"
#urlDest = "jdbc:mysql://142.150.77.152:3306/CVST_TTC?user=cvst&password=cvst"
#driverName = "com.mysql.jdbc.Driver"
#driverPath = "/home/ubuntu/mysql-connector-java-5.1.34/mysql-connector-java-5.1.34-bin.jar"
tableName = "bixi"
Prec = 3
Tol = 0.002
sampleRate = 0.6
maxSampleSize = 3
timePrec= 1

# Initialize Variables
startTime = DT.datetime.strptime(sys.argv[1], "%Y-%m-%d %H:%M:%S")
endTime = DT.datetime.strptime(sys.argv[2], "%Y-%m-%d %H:%M:%S")
timeInt = timedelta(hours = 4)
readInt = timedelta(hours = 24)
minTravel = 8
timeIntHr = timeInt.seconds / 60 / 60
username = environ["mysqldb_user"]
password = environ["mysqldb_pswd"]
#routeQuery = TTCSQL.getRouteSQL()

# Connect to CVST_TTC database for writing
conn = MySQLdb.connect(user=username, passwd=password,db="CVST_BIXI",host="142.150.77.153")
#conn = MySQLdb.connect(user="cvst", passwd="cvst",db="CVST_TTC",host="142.150.77.152")
curs = conn.cursor()

# Get total route number
curs.execute(BIXISQL.getRouteCount())
routeCount = int(str(curs.fetchall()[0][0]))


# Counts number of buses in each route within specified time interval
def travelTime(startInt, endInt, routeIndex):
    # Get route info
    # [route_short_name] [start_lat] [start_lon] [end_lat] [end_lon]
    curs.execute(BIXISQL.getRouteRow(routeIndex))
    route = curs.fetchall()

    if route[0][0] != 24: return # REMOVE

    # Construct SQL Query for ttc raw data table
    initQuery = BIXISQL.getTTCRawRouteSQL(tableName, startInt, endInt, route[0][0])
    #print "Initial Query: " + initQuery

    # Start Spark SQL Context
    sc = SparkContext("local", "BIXIData")
    sqlContext = SQLContext(sc)

    # Get tables from SQL Database
    #print "BQ"
    ttcRawTable = sqlContext.load(None, "jdbc", None, url=BIXISQL.getSrcUrl(), dbtable=initQuery, driver=BIXISQL.getDriverName())
    sqlContext.registerDataFrameAsTable(ttcRawTable, "rawData")
    #print ttcRawTable.count()
    if ttcRawTable.count() < 1:
        sc.stop()
        return
    #print "AQ"
    #routeTable = sqlContext.load(None, "jdbc", None, url=urlDest, dbtable=routeQuery, driver=driverName)

    # change into accessible array
    #route = routeTable.collect()

    #idList = sqlContext.sql("SELECT DISTINCT(vehicle_id) FROM rawData").sample(False, sampleRate).collect()
    #print "idList: " + str(len(idList)) + " [" + str(route[0][0]) + "]"
    #for row in idList:
        #print row
    #    print "vehicle_id: " + str(row.vehicle_id)
    #    tempTable = sqlContext.sql("SELECT dateTime, dirTag FROM rawData WHERE vehicle_id=" + str(row.vehicle_id))
    #    print "Count: " + str(tempTable.count())
    #    print "start: "
    #    tempTable.sort(asc('dateTime')).show(n=1)
    #    print "end: "
    #    tempTable.sort(desc('dateTime')).show(n=1)

    curTime = startInt
    #print "route: " + str(route[i].route_short_name)

    # Get the upper and lower bounds for the start location of the route
    #startLatUpper = round(float(str(route[0][1])), Prec) + Tol
    #startLatLower = round(float(str(route[0][1])), Prec) - Tol
    #startLonUpper = round(float(str(route[0][2])), Prec) + Tol
    #startLonLower = round(float(str(route[0][2])), Prec) - Tol
    #endLatUpper = round(float(str(route[0][3])), Prec) + Tol
    #endLatLower = round(float(str(route[0][3])), Prec) - Tol
    #endLonUpper = round(float(str(route[0][4])), Prec) + Tol
    #endLonLower = round(float(str(route[0][4])), Prec) - Tol
    #print "start: " + str(startLatLower) + " " + str(startLatUpper) + " " + str(startLonLower) + " " + str(startLonUpper)
    #print "end: " + str(endLatLower) + " " + str(endLatUpper) + " " + str(endLonLower) + " " + str(endLonUpper) 

    # Select a sample list of bus ids
    idList = sqlContext.sql("SELECT nbBikes FROM rawData WHERE dateTime>='" + str(startInt) + "' AND dateTime<'" + str(startInt + timeInt) + "' ORDER BY nbBikes ASC").limit(maxSampleSize)
    sqlContext.registerDataFrameAsTable(idList, "idTable")
    curTime = startInt + timeInt
    while curTime < endInt:
        temp = sqlContext.sql("SELECT nbBikes FROM rawData WHERE dateTime>='" + str(curTime) + "' AND dateTime<'" + str(curTime + timeInt) + "' ORDER BY nbBikes").limit(maxSampleSize)
        idList = idList.unionAll(temp)
        curTime += timeInt
    idList.show()
    idList = idList.distinct().collect()

    # Loop through bus id list to calculate travel time
    print "Route: " + str(route[0][0])
    trvSchm = ['startTime', 'trvTime']
    trvTimeList = sqlContext.createDataFrame([('00:00:00',0)], trvSchm).limit(0)
    #newRow = sqlContext.createDataFrame([('00:00:01',1)], schema)
    #trvTimeList = trvTimeList.unionAll(newRow)

    for busrow in idList:
        print busrow.nbBikes
        temp = sqlContext.sql("SELECT dateTime FROM rawData WHERE station_id=" + str(busrow.station_id) + " ORDER BY dateTime ASC").collect()
        rangeSize = len(temp)
        print str(temp[0].dateTime) + " " + str(temp[0].dirTag)
        print "List Size: " + str(rangeSize)
        trvStart = temp[0].dateTime
        trvCount = 0
        trvSum = 0
        trvInt = int(trvStart.hour / timeIntHr) * timeIntHr
        for i in range(1, rangeSize):
            #print temp[i]
            if temp[i].dirTag != temp[i-1].dirTag:
                trvEnd = temp[i-1].dateTime #DT.datetime.strptime(temp[i-1].dateTime, "%Y-%m-%d %H:%M:%S")
                tempTrip = (trvEnd - trvStart).total_seconds() / 60  # caculate travel time in minutes
                if tempTrip > minTravel:
                    trvSum += tempTrip
                    trvCount += 1
                    #trvInt = int(trvStart.hour / timeIntHr) * timeIntHr
                    #newRow = sqlContext.createDataFrame([(trvInt, int(trvSum / trvCount))], trvSchm)
                    #trvTimeList = trvTimeList.unionAll(newRow)
                    #print "new: " + str(trvStart.hour) + " " + str(trvInt) + " " + str(tempTrip)
                trvStart = temp[i].dateTime
                if (int(trvStart.hour / timeIntHr) * timeIntHr != trvInt) and (trvCount != 0):
                #    print "trvInt: " + str(trvInt) + " " + str(trvStart.hour / timeIntHr)
                #    print "new: " + str((trvInt-1) * timeIntHr) + " " + str(trvSum/trvCount)
                #    newRow = sqlContext.createDataFrame([((trvInt-1) * timeIntHr, int(trvSum / trvCount))], trvSchm)
                #    trvTimeList = trvTimeList.unionAll(newRow)
                    print "new: " + " " + str(trvInt) + " " + str(int(trvSum / trvCount))
                    newRow = sqlContext.createDataFrame([(trvInt, int(trvSum / trvCount))], trvSchm)
                    trvTimeList = trvTimeList.unionAll(newRow)
                    trvCount = 0
                    trvSum = 0
                    trvInt = int(trvStart.hour / timeIntHr) * timeIntHr
                #    trvInt = trvStart.hour / timeIntHr
                    #print str(busrow.vehicle_id) + " > " + str(trvStart.hour) + " / " + str(timeIntHr) + " = " + str(trvInt)
            elif i == (rangeSize - 1):
                trvEnd = temp[i].dateTime
                #trvSum += (trvEnd - trvStart).total_seconds() / 60  # caculate travel time in minutes
                tempTrip = (trvEnd - trvStart).total_seconds() / 60
                if tempTrip > minTravel:
                    #trvInt = int(trvStart.hour / timeIntHr) * timeIntHr
                    #newRow = sqlContext.createDataFrame([(trvInt, int(tempTrip))], trvSchm)
                    #trvTimeList = trvTimeList.unionAll(newRow)
                    #print "new: " + str(trvInt) + " " + str(tempTrip)
                    trvSum += tempTrip
                    trvCount += 1
                    print "new: " + " " + str(trvInt) + " " + str(int(trvSum / trvCount))
                    newRow = sqlContext.createDataFrame([(trvInt, int(trvSum / trvCount))], trvSchm)
                    trvTimeList = trvTimeList.unionAll(newRow)


                #trvCount += 1
                #newRow = sqlContext.createDataFrame([(trvInt * timeIntHr, int(trvSum / trvCount))], trvSchm)
                #trvTimeList = trvTimeList.unionAll(newRow)

    trvTimeList = trvTimeList.groupBy('startTime').avg('trvTime').collect()

    for row in trvTimeList:
        tempTime = startTime + timedelta(hours = int(row.startTime))
        tempStr = "INSERT INTO BIXI_TRAVEL (station_id, startTime, travelTime) VALUES ("
        tempStr += str(route[0][0]) + ", '" + str(tempTime) + "', " + str(round(row[1], timePrec)) + ");"
        print tempStr
        curs.execute(tempStr)

    sqlContext.clearCache()
    sc.stop()
    conn.commit()
    sys.exit()  # REMOVE
    return

    # Calculate 1 travel time per 3-hr interval
    while curTime < endInt:
        idList = sqlContext.sql("SELECT nbBikes FROM rawData WHERE dateTime>='" + str(curTime) + "' AND dateTime<'" + str(curTime + timeInt) + "'").sample(False, sampleRate).collect()
        print "idList: " + str(len(idList)) + " [" + str(route[0][0]) + "]"
        for row in idList:
            tempTable = sqlContext.sql("SELECT dateTime FROM rawData WHERE station_id=" + str(row.station_id))
            print "Count: " + str(tempTable.count()) + " [" + str(row.station_id) + "]"
            print str(tempTable.sort(asc('dateTime')).collect()[0][0]) + " - " + str(tempTable.sort(desc('dateTime')).collect()[0][0])

            
        curTime += timeInt
        continue

        tempTable = sqlContext.sql("SELECT * FROM rawData WHERE dateTime>='" + str(curTime) + "' AND dateTime<'" + str(curTime + timeInt) + "'")
        sqlContext.registerDataFrameAsTable(tempTable, "results")
        startTable = sqlContext.sql("SELECT station_id, dateTime FROM results WHERE lat>=" + str(startLatLower) + \
                                   " AND lat<=" + str(startLatUpper) + " AND lon>=" + str(startLonLower) + \
                                   " AND lon<=" + str(startLonUpper) + " ORDER BY dateTime ASC")
        startTableSize = startTable.count()
        startTable = startTable.first()
        endTable = sqlContext.sql("SELECT station_id, dateTime FROM results WHERE lat>=" + str(endLatLower) + \
                                   " AND lat<=" + str(endLatUpper) + " AND lon>=" + str(endLonLower) + \
                                   " AND lon<=" + str(endLonUpper) + " ORDER BY dateTime ASC")
        endTableSize = endTable.count()
        endTable = endTable.collect()
        #print str(startTableSize) + " " + str(endTableSize)

        #STOPPED HERE        count zeros
        if endTableSize < 1 or startTableSize < 1: 
            curTime += timeInt
            continue
        #print "startTable: " + str(startTable[0]) + " " + str(startTable[1])

        # Loop to find first matching stop in endTable
        for j in range (0, endTableSize):
            #tripTime = DT.datetime.strptime(endTable[j].dateTime, "%Y-%m-%d %H:%M:%S") -  DT.datetime.strptime(startTable[1], "%Y-%m-%d %H:%M:%S")
            tripTime = endTable[j].dateTime - startTable[1]
            #print str(endTable[j].dateTime) + " - " + str(startTable[1]) + " = " + str(tripTime) + " > " + str(minTravel) + " " + str(tripTime > minTravel)
            #print str(tripTime) + " " + str(startTable[0]) + " == " + str(endTable[j].vehicle_id) + " " + str(endTable[j].vehicle_id == startTable[0])

            if (endTable[j].station_id == startTable[0]) and (tripTime > minTravel):
                #print str(endTable[j].vehicle_id) + " " +  str(tripTime)
                tempStr = "INSERT INTO BIXI_TRAVEL (station_id, startTime, travelTime) VALUES ("
                tempStr += str(route[0][0]) + ", '" + str(curTime) + "', '" + str(endTable[j].dateTime - startTable.dateTime) + "');"
                #print tempStr
                curs.execute(tempStr)
                break
        curTime += timeInt

    # Stop Spark Context
    sc.stop()
    return

# ---------- Main Function ---------------
# Loop through time range in 4-hr intervals
startInt = startTime
endInt = startTime + readInt

while endInt <= endTime:
    print str(startInt) + " - " + str(endInt)

    for i in range (0, routeCount):
        travelTime(startInt, endInt, i)
    
    # Increment read interval
    startInt = endInt
    endInt = startInt + readInt

conn.close()
