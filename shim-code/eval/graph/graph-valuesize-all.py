
from common_funcs import *
from pylab import *

micro_dir = "../data/valuesize/"


threadlist = [2,4,8,16,32, 64, 128, 256, 512]
runkinds = ["E-eventual", "E-shim"]
clients = [1024]

numseconds = 480.0
prunesecsstart = 90.0
prunesecsend = 30.0


graphlabels = {"E-shim":"Explicit", "E-eventual":"Eventual"}
formats = {140:"o", 1024:"+", 4096:"x", 2048:"^",  "P-localcheck-noread":"x--", "E-shim-noread":"o--"}
colors = {140:"red", 1024:"blue", 2048:"green", 4096:"orange",  "P-localcheck-noread":"green", "E-shim-noread":"red"}

numiterations = 5

valuesizes = [140, 1024, 2048, 4096]

thruputs = {}
latencies = {}
clatencies = {}
bandwidths = {}
visibilities = {}

for valuesize in valuesizes:
    for skind in runkinds:
        kind = skind+str(valuesize)
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
            errors = 0

            missingIteration = False
            for iteration in range(0, numiterations):
                if valuesize == 140:
                    if kind.find("eventual") != -1:
                        resultdir = "../lambda/E-eventual*-%d-*-%d" % (threads, iteration)
                    else:
                        resultdir = "../lambda/E-shim*-%d-*-%d" % (threads, iteration)
                elif kind.find("eventual") != -1:
                    resultdir = "E-eventual-*-%d-*VS%d-%d" % (threads, valuesize, iteration)
                else:
                    resultdir = "E-shim-*-%d-*VS%d-%d" % (threads, valuesize, iteration)

                result = RunStat(micro_dir+resultdir, numseconds, prunesecsstart, prunesecsend)
                result.parse_lipstats()
                
                if result.hasProblems:
                    missingIteration = True
                    exit(-1)
                    continue
                    #break

                if (result.lipstick_numwrites+result.lipstick_numreads == 0):
                    continue

                avglatency = (float(result.lipstick_total_write_latency+
                                    result.lipstick_total_read_latency)/
                              (result.lipstick_numwrites+
                               result.lipstick_numreads))
                
                clatency = (float(result.lipstick_storage_write_latency+
                                  result.lipstick_storage_read_latency)/
                            (result.lipstick_numwrites+
                             result.lipstick_numreads))

                throughput = average(result.throughputs)*len(result.throughputs)

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
                errors += result.errors

            if missingIteration:
                print "Missing!"
                #break

            thruputs[kind].append(average(thisthroughputs))
            latencies[kind].append(average(thislatencies))
            bandwidths[kind].append(average(thisbandwidths))
            clatencies[kind].append(average(thisclatencies))
            visibilities[kind].append(average(thisvisibilities))

            print threads, kind, result.errors, average(thisthroughputs), thisthroughputs

            if len(thislatencies) > 0:
                print max(max(thislatencies)/average(thislatencies), average(thislatencies)/min(thislatencies))
            #print threads, average(thislatencies), std(thislatencies), average(thisthroughputs), std(thisthroughputs)


clf()
for valuesize in valuesizes:
    for kind in runkinds:
        vkind = kind+str(valuesize)
        if kind.find("eventual") != -1:
            firstpart = "Eventual, "
        else:
            firstpart = "Shim, "

        label = firstpart+ str(valuesize)

        print valuesize, kind, thruputs[vkind], latencies[vkind]

        if kind.find("eventual") != -1:
            addon = "--"
            plot(thruputs[vkind], latencies[vkind], formats[valuesize]+addon, label=label, markeredgecolor=colors[valuesize], color=colors[valuesize],  markerfacecolor='None')
        else:
            addon = "-"
            plot(thruputs[vkind], latencies[vkind], formats[valuesize]+addon, label=label, markeredgecolor=colors[valuesize], color=colors[valuesize],  markerfacecolor='None')




#leg = legend(loc="upper left", numpoints=2, labelspacing=.25, handlelength=2, title="Record Size")
#fr = leg.get_frame()
#fr.set_lw(0)


simpleaxis(gca())

ax = gca()
ax.yaxis.grid(True, which='major')
ax.yaxis.grid(False, which='minor')

ax.set_yscale('log')
#ylim(ymax=175)

xlabel("Throughput (Kops/s)")
ylabel("Op. Latency (ms)")

savefig("out/valuesize-thruput.pdf", transparent=True, bbox_inches='tight', pad_inches=.05)


fig = figure()
figlegend = figure(figsize=(3.2, .4))

ax = fig.add_subplot(111)

plots = []
titles = []

for valuesize in valuesizes:
    for kind in runkinds:

        vkind = kind+str(valuesize)
        if kind.find("eventual") != -1:
            firstpart = "Eventual, "
        else:
            firstpart = "Shim, "
            continue

        titles.append(str(valuesize))


        print valuesize, kind, thruputs[vkind]

        plots.append(ax.plot(thruputs[vkind], latencies[vkind], formats[valuesize]+addon, label=label, markeredgecolor=colors[valuesize], color=colors[valuesize],  markerfacecolor='None'))


plots.reverse()
titles.reverse()

leg = figlegend.legend(plots, titles, loc="center", ncol=len(plots), title="Record Size (b)", handletextpad=.2)
fr = leg.get_frame()
fr.set_lw(0)
figlegend.savefig('out/valueslegend.pdf')
