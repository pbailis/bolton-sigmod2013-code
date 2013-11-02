sec=60
records=100000
size=1


for it in 0
do 
    for length in 20
    do
	for threads in 40 80
	do

            cassandra-cli -h localhost -f setup-cass-ycsb.cql
            bin/ycsb load bolton -P workloads/workloada  -p hosts=$CASS_HOST -p fieldlength=$size -p fieldcount=1 -p recordcount=$records -p bolton.explicitdistribution=constant -p bolton.constantdistribution.length=$length -s
            bin/ycsb run bolton -P workloads/workloada  -threads $threads -p hosts=$CASS_HOST -p fieldlength=$size -p fieldcount=1 -p operationcount=10000000 -p maxexecutiontime=$sec -p recordcount=$records -p bolton.explicitdistribution=constant -p bolton.constantdistribution.length=$length -s > ITLOCAL-TR$threads-L$length.txt
	    done
    done
done