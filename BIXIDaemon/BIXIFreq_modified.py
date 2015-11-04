# Daemon for aggregating bus frequency data
# and save count results to TTC_FREQ

from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
import sys
import datetime as DT
from datetime import timedelta
from os import environ
import ev
import requests
import json
# Debug
#print 'Number of arguments:', len(sys.argv), 'arguments.'
#print 'Argument List:', str(sys.argv)

# TTCFreq2.py <startTimeIn> <endTimeIn>

# Logic
# 1. Loop through specific time period
# 2. Read in input time interval from ttc raw data table
# 3. Read in TTC_ROUTES table
# 4. Loop through TTC_ROUTES table for all routes
#   4.1 Filter 1-hr of data
#   4.3 Count unique records in bus_id within time span
#   4.4 Save count result to TTC_FREQ table

# Constants
Prec = 3
Tol = 0.002

# Initialize Variables
startTime = DT.datetime.strptime(sys.argv[1], "%Y-%m-%d %H:%M:%S")
endTime = DT.datetime.strptime(sys.argv[2], "%Y-%m-%d %H:%M:%S")
timeInt = timedelta(minutes = 15)
readInt = timedelta(hours = 24)
# Counts number of buses in each route within specified time interval
def countBus(startInt, endInt):
    #if startInt < DT.datetime.strptime("2015-03-23 12:00:00", "%Y-%m-%d %H:%M:%S"): return    # ATTENTION: TAKE OUT

    # Start Spark SQL Context
    sc = SparkContext("local", "BIXIData")
    sqlContext = SQLContext(sc)

    # Query route list
    routeData = requests.get('http://portal.cvst.ca/api/0.1/ttc/routes')
    routeTable = routeData.json()
    routeSize = len(routeTable) 

    # Count bus freq for list of routes
    for route in routeTable:
        #if route.route_short_name != 85:
        #    continue
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

        # get ttc raw data table
        Period = {startTime:startInt,endTime:endInt}
        ttcRawTable = requests.get('http://portal.cvst.ca/api/0.1/ttc/routetag/'+route.route_short_name,params=Period)
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

        #insert into db
        freq_table = {routeTag:route.route_short_name, startTime:startInt, freq: int(round(freq/rpFactor))}
        r = requests.post('http://portal.cvst.ca/api/0.1/ttc_freq/store',data=freq_table) 

    # Stop Spark
    sc.stop()
    return


# ---------- Main Function ---------------
# Loop through time range in <readInt> intervals
startInt = startTime
endInt = startTime + readInt

while endInt <= endTime:
    print str(startInt) + " - " + str(endInt)
    countBus(startInt, endInt)
    # Increment read interval
    startInt = endInt
    endInt = startInt + readInt


