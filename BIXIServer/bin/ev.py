# Environment Variable Declarations
from os import environ

# MySQL Database
environ["mysqldb_user"] = "cvst_admin"
environ["mysqldb_pswd"] = "Cvst@2015"
environ["mysql_dbSrc"] = "CVST_RAW_DATA"
environ["mysql_dbDest"] = "BIXI_TTC"
environ["mysql_host"] = "142.150.77.153"
environ["mysql_srcUrl"] = "jdbc:mysql://142.150.77.153:3306/CVST_RAW_DATA?user=CVST_admin&password=Cvst@2015"
environ["mysql_destUrl"] = "jdbc:mysql://142.150.77.153:3306/BIXI_TTC?user=CVST_admin&password=Cvst@2015"

# JDBC
environ["jdbcDriverName"] = "com.mysql.jdbc.Driver"
environ["jdbcDriverPath"] = "mysql-connector-java-5.1.34/mysql-connector-java-5.1.34-bin.jar"

# Apache Spark
environ["sparkSubmit"] = "/opt/spark-1.3.1-bin-hadoop2.6/bin/spark-submit"

# Graphs
environ["colMax"] = "10"      # max number of data points for cumulative column graphs