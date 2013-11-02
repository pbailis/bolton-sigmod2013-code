
from pylab import *

convolen = -1

convolens = []
eventuals = []
explicits = []
replylens = []
convolendict = {}
bytes_per_explicit = 1

#mpl.rcParams['font.size'] = 12
mpl.rcParams['figure.figsize'] = 4, 1
#mpl.rcParams['lines.markersize'] = 14
#mpl.rcParams['lines.linewidth'] = 1.5

for line in open("conversations.out"):
    if line == "\n":
        if convolen != -1:
            convolens.append(convolen)
            if convolen not in convolendict:
                convolendict[convolen] = 0
            convolendict[convolen] += 1
        convolen = 0
    else:
        convolen += 1
        replylens.append(convolen)
        explicits.append(convolen*bytes_per_explicit+len(" ".join(line.split()[1:])))
        eventuals.append(len(line))

convolens.sort()
replylens.sort()
eventuals.sort()
explicits.sort()

explicitslabels = []
explicitsvalues = []
last = 0
for i in range(0, len(explicits)):
    if explicits[i] != last:
        explicitsvalues.append(float(last))
        explicitslabels.append(float(i)/len(explicits))
        last = explicits[i]

eventualslabels = []
eventualsvalues = []
last = 0
for i in range(0, len(eventuals)):
    if eventuals[i] != last:
        eventualsvalues.append(float(last))
        eventualslabels.append(float(i)/len(eventuals))
        last = eventuals[i]

plot(eventualsvalues, eventualslabels, label="Eventual")
plot(explicitsvalues, explicitslabels, "--", label="Explicit (Tweet + KDS)")
xlabel("ECDS Bytes per Write")
ylabel("CDF")

leg = legend(loc="lower right")
fr = leg.get_frame()
fr.set_lw(0)

ax = gca()
ax.xaxis.grid(True, which='major')
ax.xaxis.grid(False, which='minor')
ax.yaxis.grid(True, which='major')
ax.yaxis.grid(False, which='minor')
ax.set_axisbelow(True)


ax = gca()
ax.set_xscale("log")
xlim(xmin=50, xmax=10000)
savefig("twitter-storage.pdf", transparent=True, bbox_inches='tight', pad_inches=.1)

convolenslabels = []
convolensvalues = []
last = 0
for i in range(0, len(convolens)):
    if convolens[i] != last:
        convolensvalues.append(float(last))
        convolenslabels.append(float(i)/len(convolens))
        last = convolens[i]

replylenslabels = []
replylensvalues = []
last = 0

for i in range(0, len(replylens)):
    if replylens[i] != last:
        replylensvalues.append(float(last))
        replylenslabels.append(float(i)/len(replylens))
        last = replylens[i]

clf()

convolensvalues[0] = convolensvalues[1]
plot(convolensvalues, convolenslabels, label="Conversation Length")
plot(replylensvalues, replylenslabels, "--", label="Message Depth")
xlabel("Tweets")
ylabel("CDF")
leg = legend(loc="lower right")
fr = leg.get_frame()
fr.set_lw(0)

ax = gca()
ax.xaxis.grid(True, which='major')
ax.xaxis.grid(False, which='minor')
ax.yaxis.grid(True, which='major')
ax.yaxis.grid(False, which='minor')
ax.set_axisbelow(True)


ax = gca()
ax.set_xscale("log")
savefig("twitter-convos.pdf", transparent=True, bbox_inches='tight', pad_inches=.1)

print "Explicits:", average(explicits), std(explicits), explicits[int(.99*len(explicits))], max(explicits)
print "Eventuals:", average(eventuals), std(eventuals), eventuals[int(.99*len(eventuals))], max(eventuals)
print "Convos:", average(convolens), std(convolens), convolens[int(.5*len(convolens))],  convolens[int(.99*len(convolens))], max(convolens)
print "Depths:", average(replylens), std(replylens), replylens[int(.5*len(replylens))], replylens[int(.99*len(replylens))], max(replylens)




f = open("hist.txt", 'w')
f.write("BlockSize\t1\n")
mkey = max(convolendict.keys())
for i in range(0, mkey):
    if i not in convolendict:
        f.write("%d\t0\n" % i)
    else:
        f.write("%d\t%d\n" % (i, convolendict[i]))
f.close()
