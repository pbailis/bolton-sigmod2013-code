
from common_funcs import *
from pylab import *

micro_dir = "../data/micro-many/"

threadlist = [2,4,8,16,32, 64, 128, 256, 400]
runkinds = ["E-eventual", "E-shim"]
clients = [1024]

numseconds = 120.0
prunesecs = 15.0

totalseconds = numseconds-(2*prunesecs)

graphlabels = {"E-shim":"Explicit", "E-eventual":"Eventual"}
formats = {"E-shim":"o-", "E-eventual":"+-", "P-localcheck":"x-", "P-strong":"^-",  "P-localcheck-noread":"x--", "E-shim-noread":"o--"}
colors = {"E-shim":"red", "E-eventual":"blue", "P-localcheck":"green", "P-strong":"orange",  "P-localcheck-noread":"green", "E-shim-noread":"red"}

numiterations = 40

thruputs = {}
latencies = {}
clatencies = {}
bandwidths = {}
visibilities = {}

for kind in runkinds:
    thruputs[kind] = []
    latencies[kind] = []
    clatencies[kind] = []
    bandwidths[kind] = []
    visibilities[kind] = []
    for client in clients:
        print 
        print 
        print 
        print client
        print kind

        for threads in threadlist:
            thisthroughputs = []
            thislatencies = []
            thisbandwidths = []
            thisclatencies = []
            thisvisibilities = []

            missingIteration = False
            for iteration in range(1, numiterations+1):
                if kind.find("eventual") != -1:
                    resultdir = "E-eventual-%d-*-%d" % (threads, iteration)
                else:
                    resultdir = "E-shim-%d-*-%d" % (threads, iteration)

                result = RunStat(micro_dir+resultdir, numseconds, prunesecs)
                result.parse_lipstats()
                
                if result.hasProblems:
                    missingIteration = True
                    continue
                    #break

                if(result.lipstick_numwrites+result.lipstick_numreads == 0):
                    continue

                avglatency = (float(result.lipstick_total_write_latency+
                                    result.lipstick_total_read_latency)/
                              (result.lipstick_numwrites+
                               result.lipstick_numreads))
                
                clatency = (float(result.lipstick_storage_write_latency+
                                  result.lipstick_storage_read_latency)/
                            (result.lipstick_numwrites+
                             result.lipstick_numreads))

                throughput = (result.lipstick_numreads+
                              result.lipstick_numwrites)/totalseconds/1000.0

                bandwidth = (float((result.lipstick_byteswritten+
                                    result.lipstick_bytesread))/
                             (result.lipstick_numreads+
                              result.lipstick_numwrites))
                
                if(result.lipstick_visibility_resolved_reads != 0):
                    visibility = (float(result.lipstick_total_visibility_latency)/result.lipstick_visibility_resolved_reads)
                else:
                    visibility = 0

                thisthroughputs.append(throughput)
                thislatencies.append(avglatency)
                thisbandwidths.append(bandwidth)
                thisclatencies.append(clatency)
                thisvisibilities.append(visibility)

            if missingIteration:
                print "Missing!"
                #break

            thruputs[kind].append(average(thisthroughputs))
            latencies[kind].append(average(thislatencies))
            bandwidths[kind].append(average(thisbandwidths))
            clatencies[kind].append(average(thisclatencies))
            visibilities[kind].append(average(thisvisibilities))

            print threads, average(thislatencies), std(thislatencies), average(thisthroughputs), std(thisthroughputs)

clf()
for kind in runkinds:
    plot(thruputs[kind], latencies[kind], formats[kind], label=graphlabels[kind], markeredgecolor=colors[kind], color=colors[kind],  markerfacecolor='None')

simpleaxis(gca())

leg = legend(loc="upper left")
fr = leg.get_frame()
fr.set_lw(0)

xlabel("Throughput (Kops/s)")
ylabel("Average Latency (ms)")

savefig("out/micro-thruput.pdf", transparent=True, bbox_inches='tight', pad_inches=.1)

clf()
for kind in runkinds:
    print thruputs[kind]
    plot(thruputs[kind], visibilities[kind], formats[kind], label=graphlabels[kind], markeredgecolor=colors[kind], color=colors[kind],  markerfacecolor='None')

simpleaxis(gca())

leg = legend(loc="upper left")
fr = leg.get_frame()
fr.set_lw(0)

xlabel("Throughput (Kops/s)")
ylabel("Visibility Latency (ms)")

savefig("out/micro-visibility.pdf", transparent=True, bbox_inches='tight', pad_inches=.1)
            

