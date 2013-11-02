
from common_funcs import *
from time import gmtime, strftime
from os import system

thetime = strftime("%a-%d-%b-%Y-%H_%M", gmtime())
resultsdir = "results/"+thetime

system("mkdir -p "+resultsdir)
system("git log -1 --format=\"%%h;%%ad\" > %s/git-revision.txt" % (resultsdir))

#blow away old configs, etc.
#must be run first so pull works

killall_java("lipstick-hosts")
prepare_cassandra_for_lipstick("cassandra-hosts")

threads = 8

prepare_lipstick_all("lipstick-hosts")
prepare_lipstick("east-lipstick-hosts", "east-cassandra-hosts")
prepare_lipstick("west-lipstick-hosts", "west-cassandra-hosts")
enable_backend_reads()
#potential causality stuff
for variance,  prob in [(1, .88)]:
    for clients in [1024]:
        for valuesize in [1024, 4096, 16384, 65536]:
            set_valuesize(valuesize)
            print "Setting threads per server"
            set_lipstick_threads_per_server(threads)
            print "Setting clients per server"
            set_lipstick_clients_per_server(clients)
            
            prepare_cassandra_for_lipstick("cassandra-hosts")
            print "Setting variance per server"
            set_lipstick_variance_potential(variance)
            
            sleep(10)

            #GOOD BACKEND
            enable_backend_reads()
            set_backend_potential()

            enable_local_checkpointing("lipstick-hosts")
            reset_lipstick_states()
            run_lipstick_experiment(True, "%s/P-localcheck-%d-%dC-%dV-%dVS" % (resultsdir, threads, clients, variance, valuesize))

            set_lipstick_reply_prob_explicit(prob)
            
            prepare_cassandra_for_lipstick("cassandra-hosts")

            reset_lipstick_states()
            set_backend_explicit()
            run_lipstick_experiment(False, "%s/E-shim-%d-%dC-%fP-%dVS" % (resultsdir, threads, clients, prob, valuesize))

            #NOREAD BACKEND
            set_backend_potential()

            enable_local_checkpointing("lipstick-hosts")
            reset_lipstick_states()
            run_lipstick_experiment(True, "%s/P-localcheck-noread-%d-%dC-%dV%dVS" % (resultsdir, threads, clients, variance, valuesize))

            set_lipstick_reply_prob_explicit(prob)
            
            prepare_cassandra_for_lipstick("cassandra-hosts")

            reset_lipstick_states()
            set_backend_explicit()
            run_lipstick_experiment(False, "%s/E-shim-noread-%d-%dC-%fP-%dVS" % (resultsdir, threads, clients, prob, valuesize))

            reset_lipstick_states()
            set_backend_strong_potential()
            run_lipstick_experiment(True, "%s/P-strong-%d-%dC-%dV-%dVS" % (resultsdir, threads, clients, variance, valuesize))

            reset_lipstick_states()
            set_backend_eventual_potential()
            run_lipstick_experiment(True, "%s/P-eventual-%d-%dC-%dV-%dVS" % (resultsdir, threads, clients, variance, valuesize))

