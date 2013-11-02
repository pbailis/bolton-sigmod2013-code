
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


if False:

    print "Starting EC2 east hosts...",
    make_ec2_east(num_cassandra_east+num_regular_east)
    print "Done"

    print "Starting EC2 west hosts...",
    print "Done"

    
    system("mkdir -p hosts")

    hosts = get_instances()

    make_instancefile("all-hosts.txt", [h[0] for h in hosts])

    e_hosts = filter(lambda x: x[1].find("east") != -1, hosts)
    w_hosts = filter(lambda x: x[1].find("west") != -1, hosts)

    ec_hosts = e_hosts[:num_cassandra_east]
    el_hosts = e_hosts[num_cassandra_east:]

    wc_hosts = w_hosts[:num_cassandra_east]
    wl_hosts = w_hosts[num_cassandra_east:]

    make_instancefile("cassandra-hosts.txt", [h[0] for h in ec_hosts+wc_hosts])
    make_instancefile("lipstick-hosts.txt", [h[0] for h in el_hosts+wl_hosts])
    make_instancefile("east-cassandra-hosts.txt", [h[0] for h in ec_hosts])
    make_instancefile("east-lipstick-hosts.txt", [h[0] for h in el_hosts])
    make_instancefile("west-cassandra-hosts.txt", [h[0] for h in wc_hosts])
    make_instancefile("west-lipstick-hosts.txt", [h[0] for h in wl_hosts])
    print "Done"

    exit(-1)

    sleep(30)

print "Enabling root SSH...", 
run_script("all-hosts", "scripts/enable_root_ssh.sh", "ubuntu")
print "Done"

#print "Setting up ephemeral RAID 0...", 
run_script("all-hosts", "scripts/set_up_raid0_ephemeral.sh")
#print "Done"

#print "Fixing host file bugs...",
run_script("all-hosts", "scripts/fix-hosts-file.sh")
#print "Done"

run_cmd("all-hosts", "killall java")
run_cmd("all-hosts", "rm -rf /mnt/md0/*")

run_cmd("all-hosts", "apt-get -q -y install ntp")
run_cmd("all-hosts", "ntpd -q")



#run_cmd("all-hosts", "cd ~ && rm -rf cassandra-pbs && git clone https://github.com/pbailis/cassandra-pbs.git && cd cassandra-pbs && git checkout for-cassandra && ant jar", "ubuntu")
#run_cmd("all-hosts", "apt-get install -q -y git emacs && wget ftp://apache.cs.utah.edu/apache.org/cassandra/1.1.6/apache-cassandra-1.1.6-bin.tar.gz && tar -xvf apache* && mv apache-cassandra-1.1.6 cassandra && git clone https://github.com/brianfrankcooper/YCSB.git && apt-get  -q -y purge openjdk* && add-apt-repository ppa:webupd8team/java && apt-get -q -y update && apt-get -q -y install oracle-java7-installer && apt-get -q -y install maven2 && cd YCSB && wget http://repo1.maven.org/maven2/asm/asm-all/3.1/asm-all-3.1.jar && mvn package; mv asm-all-3.1.jar ~/.m2/repository/asm/asm/3.1/asm-3.1.jar && mvn package")

setup_ycsb_scripts("cassandra-hosts")
set_up_cassandra_ring()
launch_cassandra_ring()
#prepare_cassandra_for_lipstick("cassandra-hosts")
