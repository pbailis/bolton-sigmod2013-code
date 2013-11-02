
num_cassandra_east = 1
num_regular_east = 0
num_cassandra_west = 0
num_regular_west = 0

from common_funcs import *

from time import sleep
from os import system

#todo-verify

eastAMI = "ami-4dad7424"#BOLTON: ami-8ee848e7"# OLDER: "ami-7268b01b"
westAMI = "ami-ecf17ddc"

eastInstanceIPs = []
westInstanceIPs = []

def make_ec2_east(n):
    if n == 0:
        return
    global eastAMI
    f = raw_input("EAST: spinning up %d instances; okay? " % n)

    if f != "Y" and f != "y":
        exit(-1)

    system("ec2-run-instances %s -n %d -g 'lipstick' --t m1.large -k 'suna-real'" % (eastAMI, n))

def make_ec2_west(n):
    if n == 0:
        return
    global westAMI
    f = raw_input("WEST: spinning up %d instances; okay? " % n)

    if f != "Y" and f != "y":
        exit(-1)

    system("ec2-run-instances %s -n %d -g 'lipstick' --t m1.large --region us-west-2 -k 'watson' -b '/dev/sdb=ephemeral0' -b '/dev/sdc=ephemeral1'" % (westAMI, n))

def get_instances():
    global eastInstanceIPs
    global westInstanceIPs

    system("rm instances.txt")
    system("ec2-describe-instances --region us-east-1 >> instances.txt")
    system("ec2-describe-instances --region us-west-2 >> instances.txt")

    ret = []

    for line in open("instances.txt"):
        line = line.split()
        if line[0] == "INSTANCE":
            ip = line[3]
            if ip == "terminated":
                continue
            status = line[5]
            if status.find("shutting") != -1:
                continue
            region = line[10]
            ret.append((ip, region))

    #OUTPUT all-hosts.txt, cassandra-hosts.txt, lipstick-hosts.txt, east-cassandra-hosts.txt, east-lipstick-hosts.txt, west-cassandra-hosts.txt west-lipstick-hosts.txt

    system("rm instances.txt")

    return ret

def make_instancefile(name, hosts):
    f = open("hosts/"+name, 'w')
    for host in hosts:
        f.write("%s\n" % (host))
    f.close


setup_ycsb_scripts("all-hosts")
set_up_cassandra_ring()
launch_cassandra_ring()
