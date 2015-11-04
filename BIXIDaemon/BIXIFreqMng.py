# Daemon Manager for TTCFreq.py
# Calls TTCFreq.py periodically to process bus freq data
# USE: TTCFreqMng.py <mode> <startTime> <endTime>

# Shell Command to start Spark program
# /opt/spark-1.3.0-bin-hadoop2.4/bin/spark-submit \
#    --conf spark.executor.memory="3g" \
#    --conf spark.storage.memoryFraction="0.5" \
#    --master local[4] \
#    TTCFreq.py <startTime> <endTime>

#from __future__ import print_function
import sys
import subprocess
import datetime as DT
from datetime import timedelta
from os import environ

# Import environment variables
import ev

# Constants
logFile = environ["freqLog"]
prgName = "/home/ubuntu/bixianalytics/BIXIDaemon/BIXIFreq.py"
errFile = environ["errLog"]
schedFile = environ["schedLog"]
sparkSubmit = environ["sparkSubmit"]
#username = environ["mysqldb_user"]
#password = environ["mysqldb_pswd"]
timeInt = timedelta(hours = 24)



# MANUAL MODE: user specified <startTime> and <endTime>
def modeManual(arg1, arg2):
    startTime = DT.datetime.strptime(arg1, "%Y-%m-%d %H:%M:%S")
    endTime = DT.datetime.strptime(arg2, "%Y-%m-%d %H:%M:%S")
    startInt = startTime

    while (startInt + timeInt) <= endTime:
        # Print program start time to log
        fp = open(logFile, 'a')
        fp.write("[" + str(DT.datetime.utcnow()) + "] " + str(startInt) + " - " + str(startInt + timeInt) + " >> START\n")
        fp.close()

        # Call and wait for program to finish
        p = subprocess.Popen([sparkSubmit, '--master', 'local[4]', prgName, '%s' %(str(startInt)), '%s'%(str(startInt + timeInt))])
        p.wait()

        # Print program end time to log
        fp = open(logFile, 'a')
        if p.returncode == 0:
            fp.write("[" + str(DT.datetime.utcnow()) + "] " + str(startInt) + " - " + str(startInt + timeInt) + " >> COMPLETE\n")
        else:
            fp.write("[" + str(DT.datetime.utcnow()) + "] " + str(startInt) + " - " + str(startInt + timeInt) + " >> FAILED\n")
        fp.close()

        # Increment interval start time
        startInt += timeInt
    return

# AUTO MODE: run everyday at 05:00:00 to process yesterday data
# NOTE: VM 142.150.208.138 time 05:00:00 is 01:00:00 in actual time
def modeAuto():
    today = DT.datetime.now()
    prev = today - timedelta(days = 1)
    time2 = today.strftime("%Y-%m-%d") + " 00:00:00"
    time1 = prev.strftime("%Y-%m-%d") + " 00:00:00"
    fp = open(schedFile, 'a')
    fp.write("---- BIXIFreqMng AUTO " + str(today) + " ----\n")
    #fp.write("PrevDay [" + prev.strftime("%Y-%m-%d") + " 00:00:00]\n")
    #fp.write("Today [" + today.strftime("%Y-%m-%d") + " 00:00:00]\n")
    fp.close()
    modeManual(time1, time2)

    return


# Main Function
fr = open(errFile, 'a')

if len(sys.argv) > 1:
    if sys.argv[1] == 'auto':
        modeAuto()
    elif (sys.argv[1] == 'manual') and (len(sys.argv) > 3):
        #startTime = DT.datetime.strptime(sys.argv[2], "%Y-%m-%d %H:%M:%S")
        #endTime = DT.datetime.strptime(sys.argv[3], "%Y-%m-%d %H:%M:%S")
        #timeInt = timedelta(hours = 24)
        modeManual(sys.argv[2], sys.argv[3])
    else:
        fr.write(str(DT.datetime.utcnow()) + " [BIXIFreqMng.py] ERROR: Missing arguments")
        sys.exit()
else:
    fr.write(str(DT.datetime.utcnow()) + " [BIXIFreqMng.py] ERROR: Arguments required")
    sys.exit()

fr.close()
