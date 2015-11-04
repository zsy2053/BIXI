# TTC Analytics Webpage Server

from flask import Flask, render_template, request, redirect, url_for, abort, session, jsonify
import json
import collections
from pprint import pprint
import gviz_api

# Import Custom Functions
from bin import MySQLHandler


# Initialize the Flask application
app = Flask(__name__)
app.config['SECRET_KEY'] = 'F34TF$($e34D';

# Root page
@app.route('/')
def home():
    return render_template('index.htm')


# Get JSON data for specified graph type
@app.route('/ttcdata/<graphType>/<startInt>/<endInt>/<station_id>', methods=['POST'])
def getTTCData(graphType, startInt, endInt, routeTag):
    print str(startInt) + " " + str(endInt)

    if graphType == 'busfreq':
        print "-- busfreq --"
        jsonData = MySQLHandler.getFreqData(routeTag, startInt, endInt)

    elif graphType == 'freqsum':
        print "-- freqsum --"
        jsonData = MySQLHandler.getFreqSum(startInt, endInt)

    elif graphType == 'trvtime':
        print "-- trvtime --"
        jsonData = MySQLHandler.getTrvTime(startInt, endInt, routeTag)

    elif graphType == 'trvsum':
        print "-- trvsum --"
        jsonData = MySQLHandler.getTrvSum(startInt, endInt)

    elif graphType == 'busdens':
        print "-- busdens --"
        jsonData = MySQLHandler.getBusDens(startInt, endInt)

    elif graphType == 'busline':    # routeTag = busid
        print "-- busline --"
        jsonData = MySQLHandler.getBusLine(startInt, endInt, routeTag)

    else:
        print "ERROR: @app.route('/ttcdata/<graphType>/<startInt>/<endInt>/<station_id>')"
        return

    #print jsonData
    return jsonData


# Get route full name based on route number
@app.route('/ttcname/<station_id>', methods=['POST'])
def getTTCName(station_id):
    print "-- station name --"
    name = MySQLHandler.getRouteName(station_id)
    return name


# Get full list of routes
@app.route('/ttclist', methods=['POST'])
def getTTCList():
    print "-- station list --"
    jsonData = MySQLHandler.getRouteList()
    return jsonData


if __name__ == '__main__':
    app.run(
        host="10.12.6.24",
        port=int("8000"),
        debug=True
    )
