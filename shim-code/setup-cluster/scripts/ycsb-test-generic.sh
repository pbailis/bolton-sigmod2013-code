sec=90
records=10000

for threads in 10 50
do
    for size in 10 1000 10000
    do
     for thru in 500 1000 2000 4000 8000 16000
     do
         pkill -9 java
         rm -rf /mnt/md0/cassandra/
         bin/cassandra
         sleep 10
         bin/cassandra-cli -h $CASS_HOST -f setup-cass-ycsb.cql
         ../YCSB/bin/ycsb load cassandra-10 -P ../YCSB/workloads/workloada -p hosts=$CASS_HOST -p fieldlength=$size -p fieldcount=1 -p recordcount=$records -s
         timeout $(($sec*2)) ../YCSB/bin/ycsb run cassandra-10 -P ../YCSB/workloads/workloada  -threads $threads -target $thru -p hosts=$CASS_HOST -p fieldlength=$size -p fieldcount=1 -p operationcount=$(($sec*$thru)) -p recordcount=$r\
ecords -s > TR$threads-TH$thru-S$size.txt
     done
    done
done