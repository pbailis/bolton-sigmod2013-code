pscp -l ubuntu -h hosts/all-hosts.txt ../cassandra-pbs.zip /home/ubuntu
pssh -l ubuntu -h hosts/all-hosts.txt 'rm -r cassandra-pbs'
pssh -l ubuntu -h hosts/all-hosts.txt 'unzip cassandra-pbs.zip'
pssh -l root -h hosts/all-hosts.txt 'apt-get -q -y install ntp'
pssh -l root -h hosts/all-hosts.txt 'ntpd -q'