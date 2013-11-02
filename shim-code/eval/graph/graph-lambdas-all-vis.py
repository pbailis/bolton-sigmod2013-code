
from common_funcs import *
from pylab import *

micro_dir = "../data/lambda/"

eventualstr = "0"

threadlist = [2,4,8,16,32, 64, 128, 256, 512]
runkinds = ["E-shim"]
clients = [1024]

numseconds = 480.0
prunesecsstart = 180.0
prunesecsend = 30.0


graphlabels = {"E-shim":"Explicit", "E-eventual":"Eventual"}
formats = {10:"o-", 25:"+-", 50:"x-", 100:"^-", eventualstr:"v-", "E-shim-noread":"o--"}
colors = {10:"red", 25:"blue", 50:"green", 100:"orange",  eventualstr:"black"}#"P-localcheck-noread":"green", "E-shim-noread":"red"}

numiterations = 5

lmbdas = [eventualstr, 10, 25, 50, 100]

thruputs = {}
latencies = {}
clatencies = {}
bandwidths = {}
visibilities = {}

for lmbda in lmbdas:
    for skind in runkinds:
        kind = skind+str(lmbda)
        thruputs[kind] = []
        latencies[kind] = []
        clatencies[kind] = []
        bandwidths[kind] = []
        visibilities[kind] = []

        for threads in threadlist:
            thisthroughputs = []
            thislatencies = []
            thisbandwidths = []
            thisclatencies = []
            thisvisibilities = []

            missingIteration = False
            for iteration in range(0, numiterations):
                '''
                if lmbda == 10:
                    resultdir = "../micro-many/E-shim-%d-*-%d" % (threads, iteration+1)
                elif lmbda == eventualstr:
                    resultdir = "../micro-many/E-eventual-%d-*-%d" % (threads, iteration+1)
                '''
                if lmbda == eventualstr:
                    resultdir = "E-eventual-*-%d-*LMB%d-%d" % (threads, 10, iteration)
                else:
                    resultdir = "E-shim-*-%d-*LMB%d-%d" % (threads, lmbda, iteration)

                result = RunStat(micro_dir+resultdir, numseconds, prunesecsstart, prunesecsend)
                result.parse_lipstats()
                
                if result.hasProblems:
                    missingIteration = True
                    break

                avglatency = (float(result.lipstick_total_write_latency+
                                    result.lipstick_total_read_latency)/
                              (result.lipstick_numwrites+
                               result.lipstick_numreads))
                
                clatency = (float(result.lipstick_storage_write_latency+
                                  result.lipstick_storage_read_latency)/
                            (result.lipstick_numwrites+
                             result.lipstick_numreads))

                print result.throughputs
                throughput = average(result.throughputs)*len(result.throughputs)#(result.lipstick_numreads+
                             # result.lipstick_numwrites)/(float(result.totalms)/result.totalentries)

                bandwidth = (float((result.lipstick_byteswritten+
                                    result.lipstick_bytesread))/
                             (result.lipstick_numreads+
                              result.lipstick_numwrites))
                
                if(result.lipstick_visibility_resolved_reads != 0):
                    visibility = (float(result.lipstick_total_visibility_latency))/result.lipstick_visibility_resolved_reads
                else:
                    visibility = 0

                thisthroughputs.append(throughput)
                thislatencies.append(avglatency)
                thisbandwidths.append(bandwidth)
                thisclatencies.append(clatency)
                thisvisibilities.append(visibility)

                print throughput

            if missingIteration:
                print "Missing!"
                break

            thruputs[kind].append(average(thisthroughputs))
            latencies[kind].append(average(thislatencies))
            bandwidths[kind].append(average(thisbandwidths))
            clatencies[kind].append(average(thisclatencies))
            visibilities[kind].append(average(thisvisibilities))

            print kind, lmbda, threads
            print threads, average(thislatencies), std(thislatencies), average(thisthroughputs), std(thisthroughputs)



mpl.rcParams['figure.figsize'] = 4, 1

lmbdas.reverse()
for kind in runkinds:
    for lmbda in lmbdas:
        vkind = kind+str(lmbda)
        print visibilities[vkind]

        toplot = visibilities[vkind]
        plot(thruputs[vkind], toplot, formats[lmbda], label=str(lmbda), markeredgecolor=colors[lmbda], color=colors[lmbda],  markerfacecolor='None')

simpleaxis(gca())

#leg = legend(bbox_to_anchor=(0.14, .98, 1., .0), loc=3, ncol=4, handletextpad = .5)
#fr = leg.get_frame()
#fr.set_lw(0)
#ylim(ymax=500)
ax = gca()
ax.yaxis.grid(True, which='major')
ax.yaxis.grid(False, which='minor')

ax.set_yscale('log')



xlabel("Throughput (Kops/s)")
ylabel("Vis. Latency (ms)")

savefig("out/lmbda-visibility.pdf", transparent=True, bbox_inches='tight', pad_inches=.05)

clf()

fig = figure()
figlegend = figure(figsize=(3.2, .4))

ax = fig.add_subplot(111)

plots = []
titles = []
for kind in runkinds:
    for lmbda in lmbdas:
        vkind = kind+str(lmbda)

        titles.append(str(lmbda))
        plots.append(ax.plot(thruputs[vkind], latencies[vkind], formats[lmbda], label=str(lmbda), markeredgecolor=colors[lmbda], color=colors[lmbda],  markerfacecolor='None'))

leg = figlegend.legend(plots, titles, loc="center", ncol=len(plots), title="Mean History Length", handletextpad=.2)
fr = leg.get_frame()
fr.set_lw(0)
figlegend.savefig('out/lambdasvislegend.pdf')
