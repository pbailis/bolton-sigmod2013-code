
import time
import os
import sys

start = int(round(time.time() * 1000))

while True:
    now = int(round(time.time() * 1000))
    print now, now-start, os.system("~/apache-cassandra-1.1.0/bin/nodetool -h %s cfstats" % (sys.argv[1]))
    time.sleep(20)
