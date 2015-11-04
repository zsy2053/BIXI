# Program for computing geographical areas with high bus count
# RUNTIME: run upon user request
# USE: TTCPoint.py <startTimeIn> <endTimeIn> <fileName>

# LOGIC
# 1. Read lat and lon data for specified time period <startTime> <endTime>
# 2. Group lat and lon pairs close to each other (within tolerance <Tol>)
# 3. Compute top <maxSampleSize> most frequently occurring lat and lon pairs

# Import Spark Libraries
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *

# Import Python Libraries
import sys
import datetime as DT
from math import hypot
from decimal import Decimal
from os import environ

# Parameters
Prec = 4
Tol = 0.002     # ~200m radius
sampleRate = 0.5
maxSampleSize = 20

# Initialize Variables
startTime = DT.datetime.strptime(sys.argv[1], "%Y-%m-%d %H:%M:%S")
endTime = DT.datetime.strptime(sys.argv[2], "%Y-%m-%d %H:%M:%S")
fileName = sys.argv[3]
driverName = environ["jdbcDriverName"]
dbSrcUrl = environ["mysql_srcUrl"]
dbDestUrl = environ["mysql_destUrl"]


# Start Spark SQL Context
sc = SparkContext("local", "BIXIData")
sqlContext = SQLContext(sc)

# Query lat and lon data
initQuery = "(SELECT lat, lon FROM BIXI_STATIONS WHERE dateTime>='" + str(startTime) + "' AND dateTime<'" + str(endTime) + "') AS T"
pointTable = sqlContext.load(None, "jdbc", None, url=dbSrcUrl, dbtable=initQuery, driver=driverName).sample(False, sampleRate)

# Round lat and lon data to precision <Prec>
roundCol = udf(lambda a: str(round(a, Prec)), StringType())
results = pointTable.withColumn('latR', roundCol(pointTable.lat)).withColumn('lonR', roundCol(pointTable.lon)).select('latR', 'lonR')

# Group lat and lon data and count within each group, pick the top <maxSampleSize>*2 groups with the highest bus count
temp = results.groupBy(results.latR, results.lonR).count().sort(desc('count')).limit(maxSampleSize * 2).sort(asc('latR'), asc('lonR')).collect()

# Group points that are close to each other
# based on comparing euclidean distance between lat and lon pairs to the tolerance <Tol>
counter = 0
pointList = []
point = {}

for i in range (0, len(temp)):
    if counter < 1:  # populate empty list
        point['lat'] = temp[i].latR
        point['lon'] = temp[i].lonR
        point['count'] = temp[i].count
        pointList.append(point.copy())
        counter += 1
    if counter >= maxSampleSize:  # obtained all samples
        break
    elif hypot(Decimal(temp[i].latR) - Decimal(point['lat']), Decimal(temp[i].lonR) - Decimal(point['lon'])) < Tol:
        pointList[counter-1]['count'] += temp[i].count
    else:
        point['lat'] = temp[i].latR
        point['lon'] = temp[i].lonR
        point['count'] = temp[i].count
        pointList.append(point.copy())
        counter += 1


# Construct output JSON string
#[{"lat":<float>, "lon":<float>, "count":<int>}]
jsonStr = '['
i = 0
for r in pointList:
    if i != 0 : jsonStr += ', '
    #tempStr = r.point.split()
    #jsonStr += '{"lat":' + tempStr[0] + ', "lon":' + tempStr[1] + ', "count":' + str(r.count * 2) + '}'
    jsonStr += '{"lat":' + str(r['lat']) + ', "lon":' + str(r['lon']) + ', "count":' + str(r['count'] * 2) + '}'
    i += 1

jsonStr += ']'
#print jsonStr

# Stop Spark
sc.stop()

# Save JSON data to file
f = open(fileName,'w')
f.write(jsonStr)
f.close()

