
from common_funcs import *
from time import gmtime, strftime
from os import system

thetime = strftime("%a-%d-%b-%Y-%H_%M", gmtime())
resultsdir = "results/longrun-"+thetime

runtime=15*60

system("mkdir -p "+resultsdir)
system("git log -1 --format=\"%%h;%%ad\" > %s/git-revision.txt" % (resultsdir))

#blow away old configs, etc.
#must be run first so pull works

killall_java("lipstick-hosts")
#prepare_cassandra_for_lipstick("cassandra-hosts")
prepare_lipstick_all("lipstick-hosts")
prepare_lipstick("east-lipstick-hosts", "east-cassandra-hosts")
prepare_lipstick("west-lipstick-hosts", "west-cassandra-hosts")

threads = 8
clients = 1024
variance = 1

print "Setting threads per server"
set_lipstick_threads_per_server(threads)
print "Setting clients per server"
set_lipstick_clients_per_server(clients)
enable_backend_reads()

print "Setting variance per server"
set_lipstick_variance_potential(variance)

set_backend_potential()
    
enable_local_checkpointing("lipstick-hosts")

#potential causality stuff
reset_lipstick_states()

run_lipstick_experiment(True, "%s/P-localcheck-%d-%dC-%dV" % (resultsdir, threads, clients, variance), runtime)

set_backend_explicit()
    
disable_local_checkpointing("lipstick-hosts")

run_lipstick_experiment(False, "%s/E-shim-%d-%dC-%dV" % (resultsdir, threads, clients, variance), runtime)
