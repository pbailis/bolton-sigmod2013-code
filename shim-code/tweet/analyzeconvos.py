
count = 0
numconvos = 0
numtweets = 0
for line in open("conversations.out"):
    if line == "\n":
        #print count
        count = 0
        numconvos += 1
    else:
        count+= 1
        numtweets += 1

print numconvos, numtweets
