
from common_funcs import *
from time import gmtime, strftime
from os import system

thetime = strftime("%a-%d-%b-%Y-%H_%M", gmtime())
resultsdir = "results/micro-many-explicit/"+thetime

system("mkdir -p "+resultsdir)
system("git log -1 --format=\"%%h;%%ad\" > %s/git-revision.txt" % (resultsdir))

#blow away old configs, etc.
#must be run first so pull works

killall_java("lipstick-hosts")
#killall_java("cassandra-hosts")
#set_up_cassandra_ring()
#prepare_cassandra_for_lipstick("cassandra-hosts")

prepare_lipstick_all("lipstick-hosts")
prepare_lipstick("east-lipstick-hosts", "east-cassandra-hosts")
prepare_lipstick("west-lipstick-hosts", "west-cassandra-hosts")
enable_backend_reads()

#set_lipstick_shim_param_all("lipstick-hosts", "storage.class", "edu.berkeley.lipstick.storage.DummyStorage")

clients = 1024

#potential causality stuff
for variance,  prob_reply, chain_lambda  in [(1, .3, 10.0), (1, .3, 20.0)]:
    set_lipstick_variance_potential(variance)
    set_lipstick_reply_prob_explicit(prob_reply)
    set_lipstick_chain_lambda(chain_lambda)
    #enable_local_checkpointing("lipstick-hosts")
    enable_backend_reads()
    set_lipstick_clients_per_server(clients)
    for iteration in range(0, 5):
        for pgratio in [.5]:#0.058824, .2, .5]:
            set_lipstick_putget_ratio(pgratio)
            for threads in [2,4,8,16,32,64,128,256]:    

                set_lipstick_threads_per_server(threads)
                reset_lipstick_states()
                set_backend_explicit()
                run_lipstick_experiment(False, "%s/E-shim-pgr%f-%d-%dC-%fP-%dL-%d" % (resultsdir, pgratio, threads, clients, prob_reply, chain_lambda, iteration), 120)
                
                reset_lipstick_states()
                set_backend_eventual_explicit()
                run_lipstick_experiment(True, "%s/E-eventual-pgr%f-%d-%dC-%dV-%d" % (resultsdir, pgratio, threads, clients, variance, iteration), 120)

                continue

                reset_lipstick_states()
                set_backend_strong_explicit()
                run_lipstick_experiment(True, "%s/E-strong-pgr%f-%d-%dC-%dV-%d" % (resultsdir, pgratio, threads, clients, variance, iteration), 120)
                
                                
                '''
                reset_lipstick_states()
                set_backend_potential()
                run_lipstick_experiment(True, "%s/P-localcheck-%d-%dC-%dV-%d" % (resultsdir, threads, clients, variance, iteration))
                '''

