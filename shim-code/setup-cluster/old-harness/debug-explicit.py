


from common_funcs import *
from time import gmtime, strftime
from os import system

thetime = strftime("%a-%d-%b-%Y-%H_%M", gmtime())
resultsdir = "results/debug-explicit/"+thetime

system("mkdir -p "+resultsdir)
system("git log -1 --format=\"%%h;%%ad\" > %s/git-revision.txt" % (resultsdir))

#blow away old configs, etc.
#must be run first so pull works

killall_java("lipstick-hosts")
killall_java("cassandra-hosts")
set_up_cassandra_ring()
prepare_cassandra_for_lipstick("cassandra-hosts")


prepare_lipstick_all("lipstick-hosts")
prepare_lipstick("east-lipstick-hosts", "east-cassandra-hosts")
prepare_lipstick("west-lipstick-hosts", "west-cassandra-hosts")
enable_backend_reads()

clients = 1024

#potential causality stuff
for variance,  prob in [(1, .88)]:
    set_lipstick_variance_potential(variance)
    set_lipstick_reply_prob_explicit(prob)
    enable_local_checkpointing("lipstick-hosts")
    enable_backend_reads()
    set_lipstick_clients_per_server(clients)
    #set_lipstick_shim_param_all("lipstick-hosts", "backend.async.sleepms", "900000")
    for iteration in range(0, 1):
        for threads in [8, 16, 32]:    

            set_lipstick_threads_per_server(threads)

            reset_lipstick_states()
            set_backend_explicit()
            run_lipstick_experiment(False, "%s/E-shim-%d-%dC-%fP-%d" % (resultsdir, threads, clients, prob, iteration))
