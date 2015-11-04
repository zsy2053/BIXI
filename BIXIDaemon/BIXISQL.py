# Construct SQL Queries Statements
from os import environ

# Initialize Variables
username = environ["mysqldb_user"]
password = environ["mysqldb_pswd"]

# Connection String for CVST_RAW_DATA database
# Used read-only account to prevent accidental write
def getSrcUrl():
    return "jdbc:mysql://142.150.77.153:3306/CVST_RAW_DATA?user=" + username + "&password=" + password

# Connection String for CVST_TTC database
# Used write account
def getDestUrl():
    return "jdbc:mysql://142.150.77.153:3306/CVST_BIXI?user=" + username + "&password=" + password

# JDBC driver name for MySQL connections
def getDriverName():
    return "com.mysql.jdbc.Driver"

# JDBC driver location
def getDriverPath():
    return "/home/ubuntu/mysql-connector-java-5.1.34/mysql-connector-java-5.1.34-bin.jar"

# Construct SQL Query for ttc raw data table
# (SELECT * FROM <table> WHERE dateTime >= '<date> <time>' AND dateTime < '<date> <time>' ) AS T
def getTTCRawSQL(tableName, startTimeIn, endTimeIn):
    queryStr = "(SELECT * FROM " + tableName + " WHERE "      # initial query
    queryStr += "dateTime >= '" + str(startTimeIn) + "' "     # add time lower bound
    queryStr += "AND dateTime < '" + str(endTimeIn) + "' "    # add time upper bound
    queryStr += ") AS T"                                      # add query ending
    return queryStr

def getTTCRawRouteSQL(tableName, startTimeIn, endTimeIn, routeTag):
    queryStr = "(SELECT * FROM " + tableName + " WHERE "      # initial query
    queryStr += "dateTime >= '" + str(startTimeIn) + "' "     # add time lower bound
    queryStr += "AND dateTime < '" + str(endTimeIn) + "' "    # add time upper bound
    queryStr += "AND routeTag = " + str(routeTag)             # add routeTag number
    queryStr += ") AS T"                                      # add query ending
    return queryStr

# Construct SQL Query for stop latitude
# SELECT stop_lat FROM TTC_STOPS WHERE stop_id = 
# (SELECT stop_id FROM TTC_STOPTIMES WHERE trip_id = 
# (SELECT trip_id FROM TTC_TRIPS WHERE route_id = 
# (SELECT route_id FROM TTC_ROUTES WHERE route_short_name = 10) LIMIT 1) LIMIT 1);
def getStopSQL(field, route_id, tag):
    queryStr = "(SELECT " + field + " FROM BIXI_STOPS WHERE stop_id = "
    queryStr += "(SELECT stop_id FROM BIXI_STOPTIMES WHERE trip_id = "
    queryStr += "(SELECT trip_id FROM BIXI_TRIPS WHERE station_id = "
    if tag == "first":
        queryStr += str(route_id) + " ORDER BY trip_id ASC LIMIT 1)"
        queryStr += " ORDER BY stop_sequence ASC LIMIT 1)) AS T"
    elif tag == "last":
        queryStr += str(route_id) + " ORDER BY trip_id DESC LIMIT 1)"
        queryStr += " ORDER BY stop_sequence DESC LIMIT 1)) AS T"
    else:
        print "ERROR: <BIXISQL.getStopSQL> stop tag must not be empty!"
    return queryStr

# Construct SQL Query for route table
# SELECT route_id, route_short_name FROM TTC_ROUTES;
def getRouteSQL():
    return "(SELECT station_id, route_short_name, start_lat, start_lon, end_lat, end_lon FROM TTC_ROUTES) AS T"

def getRouteCount():
    return "SELECT COUNT(*) FROM BIXI_ROUTES;"

def getRouteRow(index):
    return "SELECT route_short_name, start_lat, start_lon, end_lat, end_lon FROM BIXI_ROUTES ORDER BY station_id LIMIT " + str(index) + ",1;"

def getLocUpdateSQL(tbName, route_id, start_lat, start_lon, end_lat, end_lon):
    queryStr = "UPDATE " + tbName + " SET start_lat=" + str(start_lat) + ", start_lon=" + str(start_lon) + ", "
    queryStr += "end_lat=" + str(end_lat) + ", end_lon=" + str(end_lon)
    queryStr += " WHERE station_id=" + str(station_id) + ";"
    return queryStr

def getRdLocSQL(station_id):
    return "(SELECT start_lat, start_lon FROM BIXI_ROUTES WHERE station_id=" + str(station_id) + ") AS T"

def getStopSQL(stopCode):
    return "(SELECT ) AS T"

def getBIXIPointSQL(tableName, startInt, endInt):
    return "(SELECT lat, lon FROM " + tableName + \
            " WHERE dateTime>='" + str(startInt) + "' AND dateTime<'" + str(endInt) + "') AS T"
