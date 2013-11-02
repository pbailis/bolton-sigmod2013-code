
from pylab import *
from datetime import datetime

convolen = -1

iars = []

lengths = []

#mpl.rcParams['font.size'] = 12
mpl.rcParams['figure.figsize'] = 10, 6
#mpl.rcParams['lines.markersize'] = 14
#mpl.rcParams['lines.linewidth'] = 1.5

recenttime = -1
length = -1
prevline = ""
curline = ""

convoiars = []

for line in open("conversations.out"):
    curline = line
    if line == "\n":
        if len(convo):
            convoiars.append(convo)
        convo = []
        if length != -1:
            lengths.append(length)

        prevline = ""
        length = 0
        recenttime = -1
    else:
        length += 1
        line = line.split()
        time = line[len(line)-2]+" "+line[len(line)-1]
        time = datetime.strptime(time, "%Y-%m-%d %H:%M:%S")

        if recenttime != -1:
            iars.append((time-recenttime).seconds)
            convoiars.append((time-recenttime).seconds)

        prevline = curline
        recenttime = time

iars.sort()


iarsvalues = []
iarslabels = []

last = 0
for i in range(0, len(iars)):
    if iars[i] != last:
        iarsvalues.append(float(last))
        iarslabels.append(float(i)/len(iars))
        last = iars[i]

iarsvalues[0] = iarsvalues[1]
plot(iarsvalues, iarslabels)
xlabel("Interarrival (s)")
ylabel("CDF")

leg = legend(loc="lower right")


ax = gca()
ax.xaxis.grid(True, which='major')
ax.xaxis.grid(False, which='minor')
ax.yaxis.grid(True, which='major')
ax.yaxis.grid(False, which='minor')
ax.set_axisbelow(True)

ax = gca()
ax.set_xscale("log")

savefig("interarrival-convos.pdf", transparent=True, bbox_inches='tight', pad_inches=.1)


cla()

lengths.sort()


lengthsvalues = []
lengthslabels = []

last = 0
for i in range(0, len(lengths)):
    if lengths[i] != last:
        lengthsvalues.append(float(last))
        lengthslabels.append(float(i)/len(lengths))
        last = lengths[i]

lengthsvalues[0] = lengthsvalues[1]
plot(lengthsvalues, lengthslabels)
xlabel("Conversation Length (Tweets)")
ylabel("CDF")

leg = legend(loc="lower right")


ax = gca()
ax.xaxis.grid(True, which='major')
ax.xaxis.grid(False, which='minor')
ax.yaxis.grid(True, which='major')
ax.yaxis.grid(False, which='minor')
ax.set_axisbelow(True)

ax = gca()
ax.set_xscale("log")

savefig("convo-lengths.pdf", transparent=True, bbox_inches='tight', pad_inches=.1)

for delta in [10, 20, 30]:
    for convo in convos:
        for msgidx in range(1, len(convo)):
            included_msg_count = 0
            while msgidx != 0:
                
                msgidx -= 1
            
            
        

print "Lengthsvalues:", average(lengthsvalues), std(lengthsvalues), lengthsvalues[int(.5*len(lengthsvalues))], lengthsvalues[int(.99*len(lengthsvalues))], max(lengthsvalues), lengthsvalues[int(.01*len(lengthsvalues))], lengthsvalues[int(.02*len(lengthsvalues))], lengthsvalues[int(.03*len(lengthsvalues))],lengthsvalues[int(.05*len(lengthsvalues))]

show()

