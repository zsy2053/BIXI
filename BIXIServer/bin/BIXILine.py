# Program for computing bus route traveled 
# RUNTIME: run upon user request
# USE: TTCLine.py <startTimeIn> <endTime> <routeTag> <fileName>

# LOGIC
# 1. Read and sample lat and lon data for specified period <startTime> <endTime> and route <routeTag>
# 2. Compute means and standard deviations of lat and lon data sets
# 3. Compute valid ranges of lat and lon data specified by <stdDevOut>
#       where data outside of <stdDevOut> std devs of the mean are rejected
# 4. Sample lat and lon data within <sampleGroup> sub intervals of the total valid range
#       result in a total of <maxSampleSize> data points in the end
# 5. Convert results to JSON string and save to file

# Import Spark Libraries
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *

# Import Python Libraries
import sys
import datetime as DT
import numpy
from os import environ

# Parameters
Prec = 5
sampleRate = 0.5
maxSampleSize = 400
sampleGroup = 10
stdDevOut = 2

# Initialize Variables
startTime = DT.datetime.strptime(sys.argv[1], "%Y-%m-%d %H:%M:%S")
endTime = DT.datetime.strptime(sys.argv[2], "%Y-%m-%d %H:%M:%S")
station_id = sys.argv[3]
fileName = sys.argv[4]
driverName = environ["jdbcDriverName"]
dbSrcUrl = environ["mysql_srcUrl"]
dbDestUrl = environ["mysql_destUrl"]
#print sys.argv[1] + " " + sys.argv[2] + ' ' + sys.argv[3] + ' ' + sys.argv[4]


# Start Spark SQL Context
sc = SparkContext("local", "BIXIData")
sqlContext = SQLContext(sc)

# Query lat and lon data
initQuery = "(SELECT dateTime, lat, lon FROM BIXI WHERE dateTime<'" + str(endTime) + "' AND dateTime>='" + str(startTime) + "'" + \
            " AND station_id=" + str(station_id) + ") AS T"
#print initQuery
lineTable = sqlContext.load(None, "jdbc", None, url=dbSrcUrl, dbtable=initQuery, driver=driverName).sample(False, sampleRate)
sqlContext.registerDataFrameAsTable(lineTable, "lineTbl")

# Compute standard deviations and means of lat and lon data sets
latList = []
lonList = []

for row in lineTable.collect():
    latList.append(row.lat)
    lonList.append(row.lon)

latDev = numpy.std(latList)
lonDev = numpy.std(lonList)
latMean = numpy.mean(latList)
lonMean = numpy.mean(lonList)

# Compute valid ranges of lat and lon based on <stdDevOut>
maxLat = latMean + stdDevOut * latDev
minLat = latMean - stdDevOut * latDev
maxLon = lonMean + stdDevOut * lonDev
minLon = lonMean - stdDevOut * lonDev
#print str(latMean) + ' ' + str(latDev) + ' ' + str(lonMean) + ' ' + str(lonDev)
#print str(minLat) + ' ' + str(maxLat) + ' ' + str(minLon) + ' ' + str(maxLon)

if (maxLat - minLat) > (maxLon - minLon):
    valueRange = (maxLat - minLat) / sampleGroup
    colMain = 'lat'
    curVal = minLat
    maxVal = maxLat
    colSub = 'lon'
    fixMin = minLon
    fixMax = maxLon
else:
    valueRange = (maxLon - minLon) / sampleGroup
    colMain = 'lon'
    curVal = minLon
    maxVal = maxLon
    colSub = 'lat'
    fixMin = minLat
    fixMax = maxLat
#print valueRange
#print "Total: " + str(lineTable.count())

# Stratified sampling of lat and lon pairs
point = {}
pointList = []

while curVal < (maxVal - valueRange):
    results = sqlContext.sql("SELECT * FROM lineTbl WHERE " + \
                colMain + ">=" + str(curVal) + " AND " + colMain + "<" + str(curVal + valueRange) + \
                " AND " + colSub + ">=" + str(fixMin) + " AND " + colSub + "<=" + str(fixMax))
                
    popSize = results.count()
    #print "Pop Size: " + str(popSize)
    #print round(float(maxSampleSize) / sampleGroup, Prec)
    if popSize > (maxSampleSize / sampleGroup):
        newSampleRate = round(float(maxSampleSize / sampleGroup) / popSize, Prec)
    else:
        newSampleRate = 1.0
    #print str(curVal) + ' ' + str(newSampleRate)
    results = results.sample(False, newSampleRate).limit(int(maxSampleSize / sampleGroup)).collect()
    #print "sample: " + str(len(results))
    for row in results:
        point['lat'] = row.lat
        point['lon'] = row.lon
        point['dateTime'] = row.dateTime
        pointList.append(point.copy())
    curVal += valueRange


# Construct output JSON string
#[{"lat":<float>, "lon":<float>, "dateTime":<YYYY-MM-DD HH:MM:SS>}]
jsonStr = '['
i = 0
for r in pointList:
    if i != 0 : jsonStr += ', '
    jsonStr += '{"lat":' + str(r['lat']) + ', "lon":' + str(r['lon']) + ', "dateTime":"' + str(r['dateTime']) + '"}'
    i += 1

jsonStr += ']'
#print jsonStr

# Stop Spark
sc.stop()

# Save JSON data to file
f = open(fileName,'w')
f.write(jsonStr)
f.close()

