sec=60
records=100000

CASS_SSH=ec2-174-129-125-208.compute-1.amazonaws.com
CASS_HOST=10.193.185.139
size=1

function runone {
    ssh root@$CASS_SSH 'bash /home/ubuntu/cassandra/reset-cassandra.sh'
    sleep 5
    ssh root@$CASS_SSH '/home/ubuntu/cassandra/bin/cassandra-cli -h '$CASS_HOST' -f /home/ubuntu/setup-cass-ycsb.cql'
    echo "lipstick.pid: '0'" >> lipstick.yaml
    bin/ycsb load bolton -P workloads/workloada  -p hosts=$CASS_HOST -p fieldlength=$size -p fieldcount=1 -p recordcount=$records -p bolton.explicitdistribution=exponential -p bolton.exponentialdistribution.gamma=$constant -threads 10 -s
    echo "lipstick.pid: '1'" >> lipstick.yaml
    echo "backend.maxsyncECDSreads:" $depth >> lipstick.yaml
    timeout $(($sec*2)) bin/ycsb run bolton -P workloads/workloada  -threads $threads -p hosts=$CASS_HOST -p fieldlength=$size -p fieldcount=1 -p operationcount=10000000 -p maxexecutiontime=$sec -p recordcount=$records -p bolton.explicitdistribution=exponential -p bolton.exponentialdistribution.gamma=$constant -s > $1
}

for it in {0..5}
do
    for threads in 1 5 10 20 40 60 80
    do
     echo "backend.class: edu.berkeley.lipstick.backend.ECExplicitBackend" >> lipstick.yaml
     depth=0
        runone IT$it-Keventual-TR$threads.txt
    
     for constant in 1 2 5 10 20
     do
         echo "backend.class: edu.berkeley.lipstick.backend.AsynchronousReadExplicitBackend" >> lipstick.yaml
    
         for depth in 0 1 infinity
         do
          runone IT$it-K$depth-TR$threads-L$constant.txt
         done
     done
    done
done
