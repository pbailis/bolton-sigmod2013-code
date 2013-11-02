
from os import system, getpid
from time import sleep

cassandra_root_dir = "/home/ubuntu/cassandra/"
lipstick_root_dir = "/root/libfreecake/lipstick/"

lipstick_client_conf = lipstick_root_dir+"/conf/clientconfig.yaml"
lipstick_shim_conf = lipstick_root_dir+"/conf/lipstick.yaml"

def run_cmd(hosts, cmd, user="root"):
    print("pssh -t 1000 -O StrictHostKeyChecking=no -l %s -h ../hosts/%s.txt \"%s\"" % (user, hosts, cmd))
    system("pssh -t 1000 -O StrictHostKeyChecking=no -l %s -h ../hosts/%s.txt \"%s\"" % (user, hosts, cmd))


def run_cmd_bg(hosts, cmd, user="root"):
    
    print("pssh -O StrictHostKeyChecking=no -l %s -h ../hosts/%s.txt \"%s\" &" % (user, hosts, cmd))    
    system("pssh -O StrictHostKeyChecking=no -l %s -h ../hosts/%s.txt \"%s\" &" % (user, hosts, cmd))

def run_cmd_single(host, cmd, user="root"):
    system("ssh %s@%s \"%s\"" % (user, host, cmd))

def run_cmd_single_bg(host, cmd, user="root"):
    system("ssh %s@%s \"%s\" &" % (user, host, cmd))

def run_script(hosts, script, user="root"):
    system("cp %s /tmp" % (script))
    script = script.split("/")
    script = script[len(script)-1]
    system("pscp -O StrictHostKeyChecking=no -l %s -h ../hosts/%s.txt /tmp/%s /tmp" % (user, hosts, script))
    run_cmd(hosts, "bash /tmp/%s" % (script), user)

def fetch_file_single(host, remote, local, user="root"):
    system("scp %s@%s:%s %s" % (user, host, remote, local))

def put_file_single(host, local, remote, user="root"):
    system("scp %s %s@%s:%s" % (local, user, host, remote))

def get_host_ips(hosts):
    return open("../hosts/%s.txt" % (hosts)).read().split('\n')[:-1]

#### CASSANDRA STUFF

def change_cassandra_seeds(hosts, seed):
    run_cmd(hosts, "sed -i 's/          - seeds: \\\"127.0.0.1\\\"/          - seeds: \\\"%s\\\"/' %s/conf/cassandra.yaml" % (seed, cassandra_root_dir))

def get_node_ips():
    ret = []
    system("ec2-describe-instances > /tmp/instances.txt")
    system("ec2-describe-instances --region us-west-2 >> /tmp/instances.txt")
    for line in open("/tmp/instances.txt"):
        line = line.split()
        if line[0] != "INSTANCE" or line[5] != "running":
            continue
        # addr, externalip, internalip, ami
        ret.append((line[3], line[13], line[14], line[1]))
    return ret

def get_cassandra_hosts():
    ret = []
    cips = get_host_ips("cassandra-hosts")
    #argh should use a comprehension/filter; i'm tired
    for h in get_node_ips():
        if h[0] in cips:
            ret.append(h)

    return ret

def get_matching_ip(host):
    cips = get_host_ips("cassandra-hosts")
    #argh should use a comprehension/filter; i'm tired
    for h in get_node_ips():
        if h[0] == host:
            return h[1]

def change_cassandra_listen_address():
    for host_tuple in get_cassandra_hosts():
        run_cmd_single(host_tuple[0], "sed -i 's/listen_address: localhost/listen_address: %s/' %s/conf/cassandra.yaml" % (host_tuple[2], cassandra_root_dir))
        run_cmd_single(host_tuple[0], "echo export CASS_HOST=%s >> /home/ubuntu/.bashrc && echo export CASS_HOST=%s >> /root/.bashrc" % (host_tuple[2], host_tuple[2]))
        run_cmd_single(host_tuple[0], "sed -i 's/rpc_address: localhost/rpc_address: %s/' %s/conf/cassandra.yaml" % (host_tuple[0], cassandra_root_dir))
        run_cmd_single(host_tuple[0], "echo -e \\\"\\nbroadcast_address: %s\\n\\\" >> %s/conf/cassandra.yaml" % (host_tuple[1], cassandra_root_dir))

def launch_cassandra_leader(host):
    run_cmd_single(host, "%s/bin/cassandra" % (cassandra_root_dir))

def launch_cassandra_rest(hosts_array):
    for h in hosts_array:
        run_cmd_single(h, "%s/bin/cassandra" % (cassandra_root_dir))
        sleep(30)

def change_cassandra_mem(hosts):
    #run_cmd(hosts, "sed -i 's/rpc_timeout_in_ms: 10000/rpc_timeout_in_ms: 1000000/' %s/conf/cassandra.yaml" % (cassandra_root_dir))
    run_cmd(hosts, "sed -i 's/#MAX_HEAP_SIZE/MAX_HEAP_SIZE/' %s/conf/cassandra-env.sh" % (cassandra_root_dir))
    run_cmd(hosts, "sed -i 's/#HEAP_NEWSIZE/HEAP_NEWSIZE/' %s/conf/cassandra-env.sh" % (cassandra_root_dir))
    run_cmd(hosts, "sed -i 's/4G/7G/' %s/conf/cassandra-env.sh" % (cassandra_root_dir))
    run_cmd(hosts, "sed -i 's/800M/500M/' %s/conf/cassandra-env.sh" % (cassandra_root_dir))
    run_cmd(hosts, "sed -i 's/hinted_handoff_enabled: true/hinted_handoff_enabled: false/' %s/conf/cassandra.yaml" % (cassandra_root_dir))
    run_cmd(hosts, "sed -i 's/concurrent_reads: 32/concurrent_reads: 512/' %s/conf/cassandra.yaml" % (cassandra_root_dir))
    run_cmd(hosts, "sed -i 's/concurrent_writes: 32/concurrent_writes: 512/' %s/conf/cassandra.yaml" % (cassandra_root_dir))

def change_cassandra_snitch(hosts):
    #run_cmd(hosts, "sed -i 's/endpoint_snitch: SimpleSnitch/endpoint_snitch: Ec2MultiRegionSnitch/' %s/conf/cassandra.yaml" % (cassandra_root_dir))
    return

def change_cassandra_logger(hosts):
    run_cmd(hosts, "sed -i 's/log4j.appender.R.File=\/var\/log\/cassandra\/system.log/log4j.appender.R.File=\/mnt\/md0\/cassandra\/system.log/' %s/conf/log4j-server.properties" % (cassandra_root_dir))
    run_cmd(hosts, "sed -i 's/    - \/var\/lib\/cassandra\/data/    - \/mnt\/md0\/cassandra\/data/' %s/conf/cassandra.yaml" % (cassandra_root_dir))
    run_cmd(hosts, "sed -i 's/\/var\/lib\/cassandra\/commitlog/\/mnt\/md0\/cassandra\/commitlog/' %s/conf/cassandra.yaml" % (cassandra_root_dir))
    return

def kill_cassandra(hosts):
    print "Killing Cassandra..."
    run_cmd(hosts, "killall java")
    print "Done (okay if RED above)"

def clean_cassandra(hosts):
    print "Cleaning Cassandra..."
    run_cmd(hosts, "rm -rf /mnt/md0/cassandra")
    print "Done"

def check_cassandra_ring(host, desiredcnt):
    run_cmd_single(host, "%s/bin/nodetool -h localhost ring > /tmp/ring.out" % (cassandra_root_dir))
    fetch_file_single(host, "/tmp/ring.out", "/tmp")
    hostcnt = open("/tmp/ring.out").read().count("Up")
    if hostcnt != desiredcnt:
        print "Got %d expected %d!" % (hostcnt, desiredcnt)
        return False
    else:
        print "Saw all %d nodes" % (desiredcnt)
        return True

def set_up_cassandra_ring():
    #probably won't change, but let's do it anyway
    hostgroup = "cassandra-hosts"

    '''
    run_script(hostgroup, "scripts/get-latest-cassandra.sh")

    print "Installing cql...",
    run_cmd("cassandra-hosts", "apt-get install python-setuptools -q -y")
    run_cmd("cassandra-hosts", "easy_install cql")
    run_cmd("cassandra-hosts", "sed -i 's/            encoding = sys.stdout.encoding/            import locale\\n            encoding = locale.getpreferredencoding()/' apache-cassandra-1.1.0/bin/cqlsh")
    print "Done"

    '''

    run_cmd("cassandra-hosts", "pkill -9 java")
    run_cmd("cassandra-hosts", "rm -rf /mnt/md0/*")
    run_cmd("cassandra-hosts", "rm -rf /var/lib/cassandra/*")
    print "Getting host ips..."
    chosts = get_host_ips(hostgroup)
    print "Done"

    leader = chosts[0]

    leaderPublicIP = get_matching_ip(leader)

    #change seed
    print "Changing Cassandra seeds..."
    change_cassandra_seeds(hostgroup, leaderPublicIP)
    print "Done"

    print "Changing listen addresses seeds..."
    change_cassandra_listen_address()
    print "Done"

    print "Changing snitch..."
    #change_cassandra_snitch(hostgroup)
    print "Done"

    print "Changing logger..."
    change_cassandra_logger(hostgroup)
    print "Done"

    print "Changing heap..."
    #change_cassandra_mem(hostgroup)
    print "Done"

    '''
    for i in xrange(len(chosts)):
        token = 2 ** 127 / len(chosts) * i
        curnode = chosts[i]
        run_cmd_single(curnode, "sed -i 's/initial_token:/initial_token: %d/' %s/conf/cassandra.yaml" % (token, cassandra_root_dir))
    '''

def launch_cassandra_ring():

    chosts = get_host_ips("cassandra-hosts")
    print "Done"

    '''
    for i in xrange(len(chosts)):
        token = 2 ** 127 / len(chosts) * i
        curnode = chosts[i]
        run_cmd_single(curnode, "sed -i 's/initial_token:/initial_token: %d/' %s/conf/cassandra.yaml" % (token, cassandra_root_dir))
    '''

    run_cmd("cassandra-hosts", "pkill -9 java")
    run_cmd("cassandra-hosts", "rm -rf /mnt/md0/*")

    leader = chosts[0]

    print "Launching Cassandra leader..."
    launch_cassandra_leader(leader)
    sleep(5)
    print "Done"

    print "Launching other Cassandra nodes..."
    launch_cassandra_rest(chosts[1:])
    print "Done"

    if(not check_cassandra_ring(leader, len(chosts))):
        print "EXITING"
        exit(-1)

    


#### LIPSTICK STUFF

def set_lipstick_yaml_param_all(hosts, yaml, param, value):
    run_cmd(hosts, "echo -e \\\"\\n%s: %s\\\" >> %s" % (param, value, yaml))

def set_lipstick_yaml_param_one(host, yaml, param, value):
    run_cmd_single(host, "echo -e \\\"\\n%s: %s\\\" >> %s" % (param, value, yaml))

def set_lipstick_shim_param_all(hosts, param, value):
    set_lipstick_yaml_param_all(hosts, lipstick_shim_conf, param, value)

def set_lipstick_client_param_all(hosts, param, value):
    set_lipstick_yaml_param_all(hosts, lipstick_client_conf, param, value)

def set_lipstick_shim_param_one(host, param, value):
    set_lipstick_yaml_param_one(host, lipstick_shim_conf, param, value)

def set_lipstick_client_param_one(host, param, value):
    set_lipstick_yaml_param_one(host, lipstick_client_conf, param, value)

def restore_clean_yaml(hosts, yaml):
    run_cmd(hosts, "cp %s %s" % (lipstick_root_dir+"/conf/backup/"+yaml, lipstick_root_dir+"/conf"+yaml))

def enable_backend_reads():
    set_lipstick_shim_param_all("lipstick-hosts", "backend.read.localonly", "false")

def disable_backend_reads():
    set_lipstick_shim_param_all("lipstick-hosts", "backend.read.localonly", "true")

def set_checkpoint_interval(interval):
    set_lipstick_shim_param_all("lipstick-hosts", "checkpoint.local.sleep", str(interval))

def set_valuesize(size):
    set_lipstick_client_param_all("lipstick-hosts", "cops.valuesize", str(size))


def set_backend_potential():
    print "Setting potential backend"
    set_lipstick_shim_param_all("lipstick-hosts", "backend.class", "edu.berkeley.lipstick.backend.AsynchronousReadPotentialBackend")
    set_lipstick_shim_param_all("lipstick-hosts", "cassandra.consistencylevel", "ONE")

def set_backend_strong_potential():
    print "Setting strong consistency backend"
    set_lipstick_shim_param_all("lipstick-hosts", "backend.class", "edu.berkeley.lipstick.backend.ECPotentialBackend")
    set_lipstick_shim_param_all("lipstick-hosts", "cassandra.consistencylevel", "EACH_QUORUM")

def set_backend_eventual_potential():
    print "Setting eventual consistency backend"
    set_lipstick_shim_param_all("lipstick-hosts", "backend.class", "edu.berkeley.lipstick.backend.ECPotentialBackend")
    set_lipstick_shim_param_all("lipstick-hosts", "cassandra.consistencylevel", "ONE")

def set_backend_explicit():
    print "Setting explicit backend"
    set_lipstick_shim_param_all("lipstick-hosts", "backend.class", "edu.berkeley.lipstick.backend.AsynchronousReadExplicitBackend")
    set_lipstick_shim_param_all("lipstick-hosts", "cassandra.consistencylevel", "ONE")

def set_backend_strong_explicit():
    print "Setting strong consistency backend"
    set_lipstick_shim_param_all("lipstick-hosts", "backend.class", "edu.berkeley.lipstick.backend.ECExplicitBackend")
    set_lipstick_shim_param_all("lipstick-hosts", "cassandra.consistencylevel", "EACH_QUORUM")

def set_backend_eventual_explicit():
    print "Setting eventual consistency backend"
    set_lipstick_shim_param_all("lipstick-hosts", "backend.class", "edu.berkeley.lipstick.backend.ECExplicitBackend")
    set_lipstick_shim_param_all("lipstick-hosts", "cassandra.consistencylevel", "ONE")

def set_lipstick_pid(host, pid):
    print host, str(pid)
    set_lipstick_shim_param_one(host, "lipstick.pid", "\\\\\\\""+str(pid)+ "\\\\\\\"")

def set_lipstick_pids(hosts):
    ips = get_host_ips(hosts)
    
    for i in range(0, len(ips)):
        print i, ips[i]
        set_lipstick_pid(ips[i], i)

def set_lipstick_serverno(host, pid):
    set_lipstick_client_param_one(host, "server.number", pid)

def set_server_ids(hosts):
    ips = get_host_ips(hosts)
    
    for i in range(0, len(ips)):
        set_lipstick_serverno(ips[i], i)    

def restore_clean_shim_conf(hosts):
    restore_clean_yaml(hosts, "lipstick.yaml")

def restore_clean_client_conf(hosts):
    restore_clean_yaml(hosts, "clientconfig.yaml")

def set_lipstick_cassandra_ip(lhost, chost):
    set_lipstick_shim_param_one(lhost, "cassandra.node.ip", chost)

def set_lipstick_cassandra_ips(l_hosts, c_hosts):
    l_ips = get_host_ips(l_hosts)
    c_ips = get_host_ips(c_hosts)

    for i in range(0, len(l_ips)):
        set_lipstick_cassandra_ip(l_ips[i], c_ips[i % len(c_ips)])

def set_lipstick_putget_ratio(ratio):
    set_lipstick_shim_param_all("lipstick-hosts", "cops.putgetratio", "%f" %(ratio))
        
def create_lipstick_keyspaces(c_hosts):
    c_ips = get_host_ips(c_hosts)

    f = open("scripts/lipstick-create-keyspace-ec2.cli").read()
    f = f.replace("EASTNUM", str(len(get_host_ips("east-cassandra-hosts"))))
    f = f.replace("WESTNUM", str(len(get_host_ips("west-cassandra-hosts"))))

    script = open("/tmp/mkspace.cli", "w")
    script.write(f)
    script.close()

    for c_ip in c_ips:
        #print c_ip
        #put_file_single(c_ip, "scripts/lipstick-destroy-keyspace.cli", "/tmp/")
        #run_cmd_single(c_ip, "%s/bin/cassandra-cli -h %s -f /tmp/lipstick-destroy-keyspace.cli" % (cassandra_root_dir, c_ip))
        put_file_single(c_ip, "/tmp/mkspace.cli", "/tmp/")
        run_cmd_single(c_ip, "%s/bin/cassandra-cli -h %s -f /tmp/mkspace.cli" % (cassandra_root_dir, c_ip))
        put_file_single(c_ip, "scripts/lipstick-create-cf.cli", "/tmp/")
        run_cmd_single(c_ip, "%s/bin/cassandra-cli -h %s -f /tmp/lipstick-create-cf.cli" % (cassandra_root_dir, c_ip))

def setup_ycsb_scripts(c_hosts):
    c_ips = get_host_ips(c_hosts)
    for c_ip in c_ips:
        put_file_single(c_ip, "scripts/ycsb-test-generic.sh", cassandra_root_dir+"/")
        put_file_single(c_ip, "scripts/setup-cass-ycsb.cql", cassandra_root_dir+"/")


def truncate_lipstick_keyspaces(c_hosts):
    c_ips = get_host_ips(c_hosts)

    for c_ip in c_ips:
        put_file_single(c_ip, "scripts/lipstick-truncate-keyspace.cli", "/tmp/")
        run_cmd_single(c_ip, "%s/bin/cassandra-cli -h %s -f /tmp/lipstick-truncate-keyspace.cli" % (cassandra_root_dir, c_ip))
        run_cmd_single(c_ip, "%s/bin/nodetool -h %s compact lipstick lipstickkvs" % (cassandra_root_dir, c_ip))


def prepare_lipstick(l_hosts, c_hosts):
    if len(get_host_ips(l_hosts)) == 0:
        return

    run_cmd(l_hosts, "sysctl -w fs.file-max=100000")
    run_cmd(c_hosts, "sysctl -w fs.file-max=100000")
    print "Setting up Cassandra IP address mappings..."
    set_lipstick_cassandra_ips(l_hosts, c_hosts)
    print "Done"

def set_lipstick_checkpoint_server(hosts):
    # set server for everyone
    # set port for everyone
    # set ismaster for master

    masterip = get_host_ips(hosts)[0]

    set_lipstick_shim_param_all(hosts, "checkpoint.global.server.ip", masterip)

    s = "["
    for i in range(0, len(hosts)):
        if i != 0:
            s += ","
        s += "\\\\\\\""+str(i)+"\\\\\\\""
        
    s += "]"

    set_lipstick_shim_param_all(hosts, "checkpoint.global.clientIDs", s)
    set_lipstick_shim_param_all(hosts, "checkpoint.global.leader", "false")

    set_lipstick_shim_param_one(masterip, "checkpoint.global.leader", "true")

def enable_global_checkpointing(hosts):
    disable_local_checkpointing(hosts)
    set_lipstick_shim_param_all(hosts, "checkpoint.global", "true")

def disable_global_checkpointing(hosts):
    set_lipstick_shim_param_all(hosts, "checkpoint.global", "false")

def enable_local_checkpointing(hosts):
    disable_global_checkpointing(hosts)
    set_lipstick_shim_param_all(hosts, "checkpoint.local", "true")

def disable_local_checkpointing(hosts):
    set_lipstick_shim_param_all(hosts, "checkpoint.local", "false")
    
def disable_all_checkpointing(hosts):
    disable_global_checkpointing(hosts)
    disable_local_checkpointing(hosts)

def prepare_lipstick_all(l_hosts):
    killall_java(l_hosts)
    clean_and_pull_lipstick_dir(l_hosts)

    print "Building latest lipstick..."
    run_script(l_hosts, "scripts/get_build_latest_lipstick.sh")
    print "Done"

    print "Setting up KyotoCabinet path..."
    set_lipstick_shim_param_all(l_hosts, "localstore.kyoto.filepath", "/mnt/md0/lipstick.kch")
    print "Done"

    set_lipstick_shim_param_all(l_hosts, "cops.numservers", "%d" % (len(get_host_ips(l_hosts))))



    print "Setting up server and lipstick ID numbers..."
    set_lipstick_pids(l_hosts)
    set_server_ids(l_hosts)
    print "Done"

    print "Setting up checkpointing..."
    set_lipstick_checkpoint_server(l_hosts)
    print "Done"

    


def prepare_cassandra_for_lipstick(c_hosts):
    kill_cassandra(c_hosts)
    clean_cassandra(c_hosts)
    clean_and_pull_lipstick_dir(c_hosts)
    launch_cassandra_ring()

    print "Resetting Cassandra Keyspace..."
    create_lipstick_keyspaces(c_hosts)
    print "Done"


def reset_lipstick_states():
    print "Resetting lipstick states"
    killall_java("lipstick-hosts")
    truncate_lipstick_keyspaces("cassandra-hosts")

    sleep(10)


    #print "Attempting to restore old yaml..."
    #restore_clean_client_conf("lipstick-hosts")
    #restore_clean_shim_conf("lipstick-hosts")
    #print "Done trying"
    


def set_lipstick_threads_per_server(threads):
    set_lipstick_client_param_all("lipstick-hosts", "cops.threads.per.server", threads)

def set_lipstick_clients_per_server(clients):
    set_lipstick_client_param_all("lipstick-hosts", "cops.clients.per.server", clients)

def set_lipstick_variance_potential(variance):
    set_lipstick_client_param_all("lipstick-hosts", "cops.keygroupvariance", variance)

def set_lipstick_reply_prob_explicit(prob):
    set_lipstick_client_param_all("lipstick-hosts", "cops.probability.reply", prob)

def set_lipstick_chain_lambda(chain):
    set_lipstick_client_param_all("lipstick-hosts", "cops.lambda.reply.length", "%f" % (chain))

def gather_cassandra_logs(hosts, outdir):
    ips = get_host_ips(hosts)

    for ip in ips:
        thisdir = "%s/%s" % (outdir, ip)
        system("mkdir %s" % (thisdir))
        fetch_file_single(ip, "/tmp/cfstats.out", thisdir)

def gather_lipstick_logs(hosts, outdir):
    ips = get_host_ips(hosts)

    for ip in ips:
        thisdir = "%s/%s" % (outdir, ip)
        system("mkdir %s" % (thisdir))
        fetch_file_single(ip, "/tmp/lipstick.out", thisdir)
        fetch_file_single(ip, "/tmp/kyotoprint.out", thisdir)

def killall_python(hosts):
    run_cmd(hosts, "killall python")

def killall_java(hosts):
    print "Killing remote java for %s..." % (hosts),
    run_cmd(hosts, "killall java python bash")
    print "Done"

def get_cassandra_numkeys(host, outdir):
    run_cmd_single(host, "echo \\\"SELECT COUNT(*) from lipstick.lipstickkvs LIMIT 90000000;\\\" | apache-cassandra-1.1.0/bin/cqlsh %s > /tmp/rowcount.txt" % (host))
    fetch_file_single(host, "/tmp/rowcount.txt", outdir)

def start_cf_logging(hosts):
    for host in get_host_ips(hosts):
        run_cmd_single_bg(host, "python %s/bin/ec2printcfstats.py %s &> /tmp/cfstats.out" % (lipstick_root_dir, host))

def run_lipstick_experiment(potential, outdir, runtime=90):

    print "Removing tmpdir contents"
    run_cmd("lipstick-hosts", "rm -rf /tmp/*")
    print "Done"

    system("mkdir -p %s" % outdir)

    if potential:
        run_cmd_bg("lipstick-hosts", "bash %s/bin/ec2runcopspotential.sh &> /tmp/lipstick.out" % (lipstick_root_dir))
    else:
        run_cmd_bg("lipstick-hosts", "bash %s/bin/ec2runcopsexplicit.sh &> /tmp/lipstick.out" % (lipstick_root_dir))

    run_cmd_bg("lipstick-hosts", "python %s/bin/ec2printkyoto.py &> /tmp/kyotoprint.out" % (lipstick_root_dir))

    start_cf_logging("cassandra-hosts")

    sleep(runtime)

    print "Done with run! Killing hosts, gathering logs"

    system("pkill -TERM -P %d" % (getpid()))
    killall_java("lipstick-hosts")
    killall_python("cassandra-hosts")

    gather_cassandra_logs("cassandra-hosts", outdir)
    gather_lipstick_logs("lipstick-hosts", outdir)

    get_cassandra_numkeys(get_host_ips("cassandra-hosts")[0], outdir)

    print "Done gathering runs!"

def clean_and_pull_lipstick_dir(hosts):
    print "Pulling latest lipstick..."
    run_script(hosts, "scripts/pull_latest_lipstick.sh")
    print "Done"
