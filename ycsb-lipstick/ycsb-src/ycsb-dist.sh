sec=60
records=100000

CASS_SSH=ec2-50-16-52-91.compute-1.amazonaws.com
CASS_HOST=10.77.6.65
size=1

function runone {
    ssh root@$CASS_SSH 'bash /home/ubuntu/cassandra/reset-cassandra.sh'
    sleep 5
    ssh root@$CASS_SSH '/home/ubuntu/cassandra/bin/cassandra-cli -h 10.77.6.65 -f /home/ubuntu/setup-cass-ycsb.cql'
    echo "lipstick.pid: '0'" >> lipstick.yaml
    bin/ycsb load bolton -P workloads/workloada  -p hosts=$CASS_HOST -p fieldlength=$size -p fieldcount=1 -p recordcount=$records -p bolton.explicitdistribution=histogram -p bolton.histogramdistribution.file=hist/$dataset".hist" -threads 10 -s
    echo "lipstick.pid: '1'" >> lipstick.yaml
    echo "backend.maxsyncECDSreads:" $depth >> lipstick.yaml
    timeout $(($sec*2)) bin/ycsb run bolton -P workloads/workloada  -threads $threads -p hosts=$CASS_HOST -p fieldlength=$size -p fieldcount=1 -p operationcount=10000000 -p maxexecutiontime=$sec -p recordcount=$records -p bolton.explicitdistribution=histogram -p bolton.histogramdistribution.file=hist/$dataset".hist" -s > IT$it-K$depth-TR$threads-$dataset.txt
}

for it in {0..4}
do 
    for threads in 1 5 10 20 40 60 80
    do
	echo "backend.class: edu.berkeley.lipstick.backend.ECExplicitBackend" >> lipstick.yaml
	depth="NODEPTH"
	runone

        for dataset in "twitter" "tuaw" "flickr" "metafilter"
	do
	    echo "backend.class: edu.berkeley.lipstick.backend.AsynchronousReadExplicitBackend" >> lipstick.yaml
	
	    for depth in 1 5 10 1000000
	    do
		runone
	done
    done
done