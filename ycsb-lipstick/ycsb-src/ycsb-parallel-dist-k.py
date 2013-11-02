
from os import system
from time import sleep

sec=60
records=100000

size=1

outdir="dist-k"

cass_ssh="ec2-54-242-74-154.compute-1.amazonaws.com"
cass_host="10.12.186.30"

system("rm -rf %s" % outdir)
system("mkdir -p %s" % outdir)

def run_cmd_single(host, cmd, user="root"):
    print("ssh %s@%s \"%s\"" % (user, host, cmd))
    system("ssh %s@%s \"%s\"" % (user, host, cmd))

def run_cmd_single_bg(host, cmd, user="root"):
    print("ssh %s@%s \"%s\" &" % (user, host, cmd))
    system("ssh %s@%s \"%s\" &" % (user, host, cmd))

def run_cmd(hosts, cmd, user="root"):
    print("pssh -t 1000 -O StrictHostKeyChecking=no -l %s -h hosts/%s.txt \"%s\"" % (user, hosts, cmd))
    system("pssh -t 1000 -O StrictHostKeyChecking=no -l %s -h hosts/%s.txt \"%s\"" % (user, hosts, cmd))

run_cmd("lip-hosts", "cd /home/ubuntu/ycsb-here; git checkout lipstick.yaml")

lhosts = []

for line in open("hosts/lip-hosts.txt"):
    line=line[:-1]
    lhosts.append(line)

chosts = []

for line in open("hosts/cassandra-hosts.txt"):
    line=line[:-1]
    chosts.append(line)

for i in range(0, len(lhosts)):
    chost=chosts[i % len(chosts)]
    run_cmd_single(lhosts[i], "echo cassandra.node.ip: %s >> /home/ubuntu/ycsb-here/lipstick.yaml" % chost)
    run_cmd_single(lhosts[i], "echo lipstick.pid: \\\\\\\"%s\\\\\\\" >> /home/ubuntu/ycsb-here/lipstick.yaml" % (i+2))

def runone(runshim, threads, depth, dataset, outfile):
    run_cmd("lip-hosts", "pkill -9 java ssh")
    if runshim:
        run_cmd("lip-hosts", 'echo "backend.class: edu.berkeley.lipstick.backend.AsynchronousReadExplicitBackend" >> /home/ubuntu/ycsb-here/lipstick.yaml')
    else:
        run_cmd("lip-hosts", 'echo "backend.class: edu.berkeley.lipstick.backend.ECExplicitBackend" >> /home/ubuntu/ycsb-here/lipstick.yaml')

    run_cmd("lip-hosts", 'echo "backend.maxsyncECDSreads: %s" >> /home/ubuntu/ycsb-here/lipstick.yaml' % (depth))

    system("cd setup-cluster && python start_ring.py && cd ..")
    system("ssh root@%s \"/home/ubuntu/cassandra/bin/cassandra-cli -h %s -f /home/ubuntu/setup-cass-ycsb.cql\"" % (cass_host, cass_ssh))
    system('echo "lipstick.pid: \'0\'" >> lipstick.yaml')
    system('bin/ycsb load bolton -P workloads/workloada  -p hosts=127.0.0.1 -p fieldlength=%d -p fieldcount=1 -p recordcount=%d -p bolton.explicitdistribution=histogram -p bolton.histogramdistribution.file=/home/ubuntu/ycsb-here/hist/%s".hist" -threads 10 -s' % (size, records, dataset))
    system('echo "lipstick.pid: \'1\'" >> lipstick.yaml')

    for host in lhosts:
	run_cmd_single_bg(host, "cd /home/ubuntu/ycsb-here/;timeout %d bin/ycsb run bolton -P workloads/workloada  -threads %d -p hosts=127.0.0.1 -p fieldlength=%d -p fieldcount=1 -p operationcount=10000000 -p maxexecutiontime=%d -p recordcount=%d -p bolton.explicitdistribution=histogram -p bolton.histogramdistribution.file=/home/ubuntu/ycsb-here/hist/%s.hist -s > /tmp/out.txt" % (sec*2, threads, size, sec, records, dataset))

    sleep(sec*1.3)

    system("killall ssh")
    print "finished %s" % (outfile)

    od = "%s/%s" % (outdir, outfile)
    system("mkdir -p %s" % (od))
    for host in lhosts:
        system("scp root@%s:/tmp/out.txt %s/%s" % (host, od, host))

for it in range(0, 5):
    for threads in [1, 2, 5, 10, 20, 40, 60, 80]:
        dataset="twitter"
        depth="0"
        runone(False, threads, depth, dataset, "IT%d-Keventual-TR%d" % (it, threads))
        for dataset in ["twitter", "tuaw", "flickr", "metafilter"]:
            for depth in ["infinity", "0"]:
                runone(True, threads, depth, dataset, "IT%d-K%s-TR%d-DS%s" % (it, depth, threads, dataset))
