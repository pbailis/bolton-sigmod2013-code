
timecolumn = 0
lipstick_numwrites_column = 6
lipstick_numreads_column = 8
lipstick_byteswritten_column = 10
lipstick_bytesread_column = 12
lipstick_total_write_latency_column = 18
lipstick_total_read_latency_column = 20
lipstick_storage_write_latency_column = 22 
lipstick_storage_read_latency_column = 24

lipstick_total_visibility_latency_column = 26
lipstick_total_num_resolved_reads = 29


from RunStat import *


def format_dir(kind, threads, clients):
    return "%s-%d-%dC-*" % (kind, threads, clients)
def format_dir(kind, threads, clients, iteration):
    return "%s-%d-%dC-*-%d" % (kind, threads, clients, iteration)

def simpleaxis(ax):
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.get_xaxis().tick_bottom()
    ax.get_yaxis().tick_left()
    ax.xaxis.grid(False)
    ax.set_axisbelow(True)

def average(values):
    return sum(values, 0.0) / len(values)

 
def running_avg(data, runlen):
    ret = []

    run = []
    for d in data:
        run.append(d)
        if len(run) == runlen:
            ret.append(average(run))
            run.pop()

    return ret
