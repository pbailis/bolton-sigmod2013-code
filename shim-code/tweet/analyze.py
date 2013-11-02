
from pylab import *

explicits = []
eventuals = []

eventuals = [int(line) for line in open("eventual.sizes")]
explicits = [int(line) for line in open("explicit.sizes")]

eventuals.sort()
print "Eventual: avg %f, std %f, median %f, max %d, 99th: %d, sum: %d" % (average(eventuals), std(eventuals), median(eventuals), max(eventuals), eventuals[int(len(eventuals)*.99)], sum(eventuals))


explicits.sort()
print "Explicit: avg %f, std %f, median %f, max %d, 99th: %d, sum: %d" % (average(explicits), std(explicits), median(explicits), max(explicits), explicits[int(len(explicits)*.99)],  sum(explicits))

        
tweetlens = [int(line) for line in open("conversations.lengths")]

tweetlens.sort()

print "Tweet lengths: avg %f, std %f, median %f, max %d, 99th: %d" % (average(tweetlens), std(tweetlens), median(eventuals), max(eventuals), tweetlens[int(len(tweetlens)*.99)])
