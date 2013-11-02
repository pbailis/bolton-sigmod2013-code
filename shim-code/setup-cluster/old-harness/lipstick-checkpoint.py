
from common_funcs import *
from time import gmtime, strftime
from os import system

thetime = strftime("%a-%d-%b-%Y-%H_%M", gmtime())
resultsdir = "results/localcheckpointing-"+thetime

system("mkdir -p "+resultsdir)
system("git log -1 --format=\"%%h;%%ad\" > %s/git-revision.txt" % (resultsdir))

#blow away old configs, etc.
#must be run first so pull works

killall_java("lipstick-hosts")
prepare_cassandra_for_lipstick("cassandra-hosts")
prepare_lipstick_all("lipstick-hosts")
prepare_lipstick("east-lipstick-hosts", "east-cassandra-hosts")
prepare_lipstick("west-lipstick-hosts", "west-cassandra-hosts")

threads = 4
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
for checkpoint_interval in [1000, 10000]:
    for trial in range(0, 5):
        reset_lipstick_states()
        set_checkpoint_interval(checkpoint_interval)

        run_lipstick_experiment(True, "%s/P-localcheck-%d-%dC-%dV-CPI%d-%d" % (resultsdir, threads, clients, variance, checkpoint_interval, trial))

