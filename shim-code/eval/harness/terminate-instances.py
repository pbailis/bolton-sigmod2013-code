
from common_funcs import *
from os import system

f = raw_input("Terminating instances... okay? ")
if f != "Y" and f != "y":
    exit(-1)


iids = []
system("ec2-describe-instances > /tmp/instances.txt")
for line in open("/tmp/instances.txt"):
    line = line.split()
    if line[0] != "INSTANCE" or line[5] != "running":
        continue
    iids.append(line[1])

if(iids):
    system("ec2-terminate-instances %s" % " ".join(iids))

iids = []
system("ec2-describe-instances --region us-west-2 > /tmp/instances.txt")
for line in open("/tmp/instances.txt"):
    line = line.split()
    if line[0] != "INSTANCE" or line[5] != "running":
        continue
    iids.append(line[1])

if(iids):
    system("ec2-terminate-instances --region us-west-2 %s" % " ".join(iids))


system("rm -rf hosts")
