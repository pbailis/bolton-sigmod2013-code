
from os import listdir, path
from glob import glob

from common_funcs import *

def line_count(fname):
    with open(fname) as f:
        for i, l in enumerate(f):
            pass
    return i + 1

class RunStat:
    def __init__(self, resultdir, numseconds=90.0, prunesecsstart=15, prunesecsend=15):
        self.hasProblems = False
        self.prunesecsstart = prunesecsstart
        self.prunesecsend = prunesecsend
        self.numseconds = numseconds

        if len(glob(resultdir)) == 0:
            print "No such directory %s" % resultdir
            self.hasProblems = True
            return
        resultdir = glob(resultdir)[0]
        ec2dirs = []
        for e in listdir(resultdir):
            if e.find("cfstats") != -1:
                self.parse_cfstats(resultdir+"/"+e)
            elif e.find("ec2") != -1:
                ec2dirs.append(resultdir+"/"+e)

        self.ec2dirs = ec2dirs

    def parse_lipstats(self):
        if self.hasProblems:
            return

        self.totalms = 0
        self.totalentries = 0
        self.lipstick_numwrites = 0
        self.lipstick_numreads = 0
        self.lipstick_byteswritten = 0
        self.lipstick_bytesread = 0
        self.lipstick_total_write_latency = 0
        self.lipstick_total_read_latency = 0
        self.lipstick_storage_write_latency = 0
        self.lipstick_storage_read_latency = 0
        self.lipstick_total_visibility_latency = 0
<<<<<<< HEAD
        self.errors = 0
        self.lipstick_visibility_resolved_reads = 0
        self.throughputs = []
=======
        self.lipstick_total_num_resolved_reads = 0
>>>>>>> 140673489bc35eb00ff3851bf64f6c322717b78b

        for ec2dir in self.ec2dirs:
            if not path.exists(ec2dir+"/lipstick.out"):
                continue

            lastline = ""

            linenumber = 0
            #lc = line_count(ec2dir+"/lipstick.out")
            startline = None
            lastline = None
            for line in open(ec2dir+"/lipstick.out"):
                linenumber += 1
                if line.find("Exception") != -1:
                    #print ec2dir

                    if line.find("TimedOut") != -1:
                        #print line
                        self.errors += 1
                        continue
                    if line.find("TTransport") != -1:
                        self.errors += 1
                        #print line
                        continue
                    if line.find("reset") != -1:
                        self.errors +=1
                        #print line
                        continue

                    self.hasProblems = True
                    return

                
                if line.find("thrift") != -1:
                    self.errors += 1
                    #print line
                    continue

                if line.find("iteration") == -1 or line.find("SocketInput") != -1 or line.find("ExplicitClient") != -1 or line.find(".java") != -1:
                    continue

                curtime = round(int(line.split()[0].split(":")[0])/1000)

                if curtime >= self.prunesecsstart and startline is None:
                    startline = line
                elif curtime >= self.numseconds-self.prunesecsend and lastline is None:
                    lastline = line

            self.totalentries += 1
<<<<<<< HEAD
=======

>>>>>>> 140673489bc35eb00ff3851bf64f6c322717b78b
            startline= startline.split()

            lastline = lastline.split()
            
            thisms = (int(lastline[timecolumn].split(":")[0])-int(startline[timecolumn].split(":")[0]))
            self.totalms += thisms

            thisnumwrites = (int(lastline[lipstick_numwrites_column].split(";")[0])-int(startline[lipstick_numwrites_column].split(";")[0]))
            self.lipstick_numwrites += thisnumwrites

            thisnumreads = (int(lastline[lipstick_numreads_column].split(";")[0])-int(startline[lipstick_numreads_column].split(";")[0]))
            self.lipstick_numreads += thisnumreads

            self.lipstick_byteswritten += (int(lastline[lipstick_byteswritten_column].split(";")[0])-int(startline[lipstick_byteswritten_column].split(";")[0]))
            self.lipstick_bytesread += (int(lastline[lipstick_bytesread_column].split(";")[0])-int(startline[lipstick_bytesread_column].split(";")[0]))
            self.lipstick_total_write_latency += (int(lastline[lipstick_total_write_latency_column].split(";")[0])-int(startline[lipstick_total_write_latency_column].split(";")[0]))
            self.lipstick_total_read_latency += (int(lastline[lipstick_total_read_latency_column].split(";")[0])-int(startline[lipstick_total_read_latency_column].split(";")[0]))
            self.lipstick_storage_write_latency += (int(lastline[lipstick_storage_write_latency_column].split(";")[0])-int(startline[lipstick_storage_write_latency_column].split(";")[0]))
            self.lipstick_storage_read_latency += (int(lastline[lipstick_storage_read_latency_column].split(";")[0])-int(startline[lipstick_storage_read_latency_column].split(";")[0]))
            self.lipstick_total_visibility_latency += (int(lastline[lipstick_visibility_latency_column].split(";")[0])-int(startline[lipstick_visibility_latency_column].split(";")[0]))
            self.lipstick_visibility_resolved_reads += (int(lastline[lipstick_visibility_resolved_reads_column].split(";")[0])-int(startline[lipstick_visibility_resolved_reads_column].split(";")[0]))
            
<<<<<<< HEAD
            self.throughputs.append((thisnumwrites+thisnumreads)/float(thisms))
=======
            self.lipstick_total_visibility_latency += (int(lastline[lipstick_total_visibility_latency_column].split(";")[0])-int(startline[lipstick_total_visibility_latency_column].split(";")[0]))
            self.lipstick_total_num_resolved_reads += (int(lastline[lipstick_total_num_resolved_reads].split(";")[0])-int(startline[lipstick_total_num_resolved_reads].split(";")[0]))

>>>>>>> 140673489bc35eb00ff3851bf64f6c322717b78b
                
    #TODO: FIX PER-CASSANDRA NODE
    def parse_cfstats(self, f):
        for line in open(f):
            if line.find("Read Count") != -1:
                self.cass_read_count = int(line.split()[2])

            elif line.find("Write Count") != -1:
                self.cass_write_count = int(line.split()[2])

            elif line.find("Write Latency") != -1:
                self.cass_avg_write_latency = float(line.split()[2])

            elif line.find("Read Latency") != -1:
                self.cass_avg_read_latency = float(line.split()[2])

            elif line.find("Space used (total)") != -1:
                self.cass_total_space = int(line.split()[3])
        
            elif line.find("Memtable Data Size") != -1:
                self.cass_memtable_size = int(line.split()[3])

        self.cass_read_latency_tot = (self.cass_avg_read_latency*
                                      self.cass_read_count)
        self.cass_write_latency_tot = (self.cass_avg_write_latency*
                                       self.cass_write_count)

    def get_trace_data(self):
        ec2files = [open(f+"/lipstick.out") for f in self.ec2dirs]
            
        wlatencies = []
        writes = []
        wsizes = []
        
        prev_wlatency = {}
        prev_writes = {}
        prev_wsizes = {}
        for ec2f in ec2files:
            prev_wlatency[ec2f] = 0
            prev_writes[ec2f] = 0
            prev_wsizes[ec2f] = 0

        done = False
        while True:
            cur_wlatency = 0
            cur_writes = 0
            cur_wsizes = 0
            
            for ec2f in ec2files:
                cur_line = ec2f.readline()
                if cur_line == "":
                    done = True
                    break

                if done:
                    break
        
                cur_line = cur_line.split()
            
                this_wlatency = int(cur_line[lipstick_total_write_latency_column].split(';')[0])
                cur_wlatency += (this_wlatency - prev_wlatency[ec2f])
                prev_wlatency[ec2f] = this_wlatency
            
                this_writes = int(cur_line[lipstick_numwrites_column].split(';')[0])
                cur_writes += (this_writes - prev_writes[ec2f])
                prev_writes[ec2f] = this_writes

                this_wsizes = int(cur_line[lipstick_byteswritten_column].split(';')[0])
                cur_wsizes += (this_wsizes - prev_wsizes[ec2f])
                prev_wsizes[ec2f] = this_wsizes

            if done:
                break
                
            wlatencies.append(cur_wlatency)
            writes.append(cur_writes)
            wsizes.append(cur_wsizes)

        self.wlatencies = wlatencies
        self.writes = writes
        self.wsizes = wsizes
