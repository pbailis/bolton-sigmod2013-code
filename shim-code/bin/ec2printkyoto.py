

import time
import os

start = int(round(time.time() * 1000))

while True:
    now = int(round(time.time() * 1000))
    if(os.path.exists("/mnt/md0/lipstick.kch")):
        print now, now-start, os.stat("/mnt/md0/lipstick.kch")[6]
        time.sleep(1)
