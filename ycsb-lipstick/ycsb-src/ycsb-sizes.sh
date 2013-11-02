sec=60
records=100000

source ~/.bashrc
outdir=sizesout

rm -rf $outdir
mkdir -p $outdir

function runone {
    ssh root@$CASS_SSH 'bash /home/ubuntu/cassandra/reset-cassandra.sh'
    timeout 20 ssh root@$CASS_SSH "/home/ubuntu/cassandra/bin/cassandra-cli -h $CASS_HOST -f /home/ubuntu/setup-cass-ycsb.cql"
    
    timeout 120 bin/ycsb load cassandra-10 -P workloads/workloada  -p hosts=$CASS_HOST -p fieldlength=$size -p fieldcount=1 -p recordcount=$records -threads 10 -s
    timeout $(($sec*2)) bin/ycsb run cassandra-10 -P workloads/workloada  -threads $threads -p hosts=$CASS_HOST -p fieldlength=$size -p fieldcount=1 -p operationcount=10000000 -p maxexecutiontime=$sec -p recordcount=$records  -s > $outdir/IT$it-TR$threads-S$size.txt
}

for it in {0..5}
do 
    for size in 100 10 100 1000 1 2000 4000
    do
        for threads in 1 5 10 20 40 60 80 100
	do
	            runone
	done
    done
done