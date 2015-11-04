# Daemon for loading start_lat, start_lon, end_lat, end_lon
# for all rows in TTC_ROUTES, by querying MySQL tables
# superceded by TTCStop.py

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import jaydebeapi
from pyspark.sql.functions import *
import sys

# Import Custom Functions
import BIXISQL

# Constants
urlDest = "jdbc:mysql://142.150.77.152:3306/CVST_TTC?user=cvst&password=cvst"
driverName = "com.mysql.jdbc.Driver"
driverPath = "/home/ubuntu/mysql-connector-java-5.1.34/mysql-connector-java-5.1.34-bin.jar"

#print DT.datetime.strftime(startTime + timeInt + timeInt, "%H:%M:%S")

# Construct SQL Query for ttc raw data table
routeQuery = TTCSQL.getRouteSQL()

# Start Spark
sc = SparkContext("local", "BIXIData")
sqlContext = SQLContext(sc)

routeTable = sqlContext.load(None, "jdbc", None, url=urlDest, dbtable=routeQuery, driver=driverName)
routeTable.registerTempTable("routeTable")

#print TTCSQL.getStopSQL("stop_lat", routeTable.first().route_id, "first")
#tempTable = sqlContext.load(None, "jdbc", None, url=urlDest, dbtable=TTCSQL.getStopSQL("stop_lat", routeTable.first().route_id, "first"), driver=driverName)
#print tempTable.first().stop_lat
#quit()

#def getLoc(route):
    #print route.route_id
    #print TTCSQL.getStopSQL("stop_lat", route.route_id, "first")
    #tempTable = sqlContext.load(None, "jdbc", None, url=urlDest, dbtable=TTCSQL.getStopSQL("stop_lat", route.route_id, "first"), driver=driverName)
    #start_lat = tempTable.first().stop_lat
    #print start_lat
    #tempTable = sqlContext(None, "jdbc", None, url=urlDest, dbtable=TTCSQL.getStopSQL("stop_lon", route.route_id, "first"), driver=driverName)
    #start_lon = tempTable.stop_lon
    #tempTable = sqlContext(None, "jdbc", None, url=urlDest, dbtable=TTCSQL.getStopSQL("stop_lat", route.route_id, "last"), driver=driverName)
    #end_lat = tempTable.stop_lat
    #tempTable = sqlContext(None, "jdbc", None, url=urlDest, dbtable=TTCSQL.getStopSQL("stop_lon", route.route_id, "last"), driver=driverName)
    #end_lon = tempTable.stop_lon
    #routeTable.sql(TTCSQL.getLocUpdateSQL("routeTable", route.route_id, start_lat, start_lon, end_lat, end_long))

#sqlContext.sql(TTCSQL.getLocUpdateSQL("routeTable", routeTable.first().route_id, 2, 3, 4, 5))
route = routeTable.collect()
#print route[0].route_id
#print routeTable.count()

#Connect to database for writing
conn = jaydebeapi.connect(driverName, urlDest, driverPath)
curs = conn.cursor()

#curs.execute("INSERT INTO TTC_TRAVEL VALUES (0, 156, '156_0_156', '2015-03-21', '1:00', '2:00', 1234);")
#print curs.fetchall()

#print TTCSQL.getStopSQL("stop_lat, stop_lon", route[0].route_id, "last")
#quit()

for i in range(0, routeTable.count()):
    # Start lat and lon
    #tempTable = sqlContext.load(None, "jdbc", None, url=urlDest, dbtable=TTCSQL.getStopSQL("stop_lat, stop_lon", route[i].route_id, "first"), driver=driverName)
    #curs.execute("UPDATE TTC_ROUTES SET start_lat=" + str(tempTable.first().stop_lat) + ", start_lon=" + str(tempTable.first().stop_lon) + " WHERE route_id=" + str(route[i].route_id) + ";")
    # End lat and lon
    tempTable = sqlContext.load(None, "jdbc", None, url=urlDest, dbtable=TTCSQL.getStopSQL("stop_lat, stop_lon", route[i].route_id, "last"), driver=driverName)
    curs.execute("UPDATE BIXI_ROUTES SET end_lat=" + str(tempTable.first().stop_lat) + ", end_lon=" + str(tempTable.first().stop_lon) + " WHERE station_id=" + str(route[i].route_id) + ";")



quit()
#STOPPED HERE: use .sql() to insert lat lon values in route row
# sqlCtx.sql("SELECT stringLengthString('test')").collect()
#sqlContext.load(None, "jdbc", None, url=urlDest, dbtable=TTCSQL.getStopSQL("stop_lat", "37991", "first"), driver=driverName).first().stop_lat
#    route.start_lon = 
#    route.end_lat = 
#    route.end_lon = 

#routeTable.foreach(getLoc)

#result = sqlContext.load(None, "jdbc", None, url=urlDest, dbtable=TTCSQL.getStopSQL("stop_lat", "37991", "first"), driver=driverName)

#print result.first().stop_lat

quit()
