create keyspace usertable
    with placement_strategy = 'org.apache.cassandra.locator.SimpleStrategy'
    and strategy_options = {replication_factor:1};
use usertable;
create column family data;
