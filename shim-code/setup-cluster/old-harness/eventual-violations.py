
from common_funcs import *
from time import gmtime, strftime
from os import system

thetime = strftime("%a-%d-%b-%Y-%H_%M", gmtime())
resultsdir = "results/violations/"+thetime

system("mkdir -p "+resultsdir)
system("git log -1 --format=\"%%h;%%ad\" > %s/git-revision.txt" % (resultsdir))

variances = [1, 8, 64, 512]
clients = 1024
threads = 64

killall_java("lipstick-hosts")

clean_and_pull_lipstick_dir("cassandra-hosts")
run_script("cassandra-hosts", "scripts/set-up-violations.sh")
#prepare_cassandra_for_lipstick("cassandra-hosts")

prepare_lipstick_all("lipstick-hosts")
prepare_lipstick("east-lipstick-hosts", "east-cassandra-hosts")
prepare_lipstick("west-lipstick-hosts", "west-cassandra-hosts")

set_backend_eventual_potential()
set_lipstick_threads_per_server(threads)
set_lipstick_shim_param_all("lipstick-hosts", "storage.class", "edu.berkeley.lipstick.storage.PrintingPooledCassandraStorage")

iteration = 0
for variance in variances:
    iteration += 1

    set_lipstick_variance_potential(variance)
    enable_backend_reads()
    set_lipstick_clients_per_server(clients)
    outdir =  "%s/P-localcheck-%d-%dC-%dV" % (resultsdir, threads, clients, variance)

    reset_lipstick_states()
    run_lipstick_experiment(True, outdir, 300)

    ips = get_host_ips("cassandra-hosts")

    for ip in ips:
        thisdir = "%s/%s" % (outdir, ip)
        system("mkdir %s" % (thisdir))
        fetch_file_single(ip, "/mnt/md0/cassandra/system.log", thisdir)

    system("echo '%d' > %s" % (iteration, outdir+"/iterationno.txt"))

    iteration += 1
