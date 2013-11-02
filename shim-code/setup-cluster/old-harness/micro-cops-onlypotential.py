
from common_funcs import *
from time import gmtime, strftime
from os import system

thetime = strftime("%a-%d-%b-%Y-%H_%M", gmtime())
resultsdir = "results/onlypotential-"+thetime

system("mkdir -p "+resultsdir)
system("git log -1 --format=\"%%h;%%ad\" > %s/git-revision.txt" % (resultsdir))

#blow away old configs, etc.
#must be run first so pull works

killall_java("lipstick-hosts")
#prepare_cassandra_for_lipstick("cassandra-hosts")

prepare_lipstick_all("lipstick-hosts")
prepare_lipstick("east-lipstick-hosts", "east-cassandra-hosts")
prepare_lipstick("west-lipstick-hosts", "west-cassandra-hosts")
enable_backend_reads()
#potential causality stuff
for variance,  prob in [(1, .88), (32, .98),  (512, .99)]:
    for clients in [1024, 2048]:
        for threads in [2, 4, 8, 16, 32, 64, 128, 256]:

            print "Setting threads per server"
            set_lipstick_threads_per_server(threads)
            print "Setting clients per server"
            set_lipstick_clients_per_server(clients)
            
            print "Setting variance per server"
            set_lipstick_variance_potential(variance)
            
            sleep(10)

            #GOOD BACKEND
            enable_backend_reads()
            set_backend_potential()

            enable_local_checkpointing("lipstick-hosts")
            reset_lipstick_states()
            run_lipstick_experiment(True, "%s/P-localcheck-%d-%dC-%dV" % (resultsdir, threads, clients, variance))

