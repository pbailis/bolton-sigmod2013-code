package edu.berkeley.lipstick.storage;

import edu.berkeley.lipstick.config.Config;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

public class PrintingCassandraStorage implements IStorage {

    Cassandra.Client client;
    ConsistencyLevel cl;
    TTransport transport;

    Column writeColumn;
    ColumnParent writeColumnFamily;
    ColumnPath columnPath;

    long bytesWritten = 0;
    long bytesRead = 0;
    long readLatency = 0;
    long writeLatency = 0;

    public void open() throws Exception {
        TSocket sock = new TSocket(Config.getCassandraIP(), Config.getCassandraPort());
        transport = new TFramedTransport(sock);
        client = new Cassandra.Client(new TBinaryProtocol(transport));
        transport.open();

        client.set_keyspace(Config.getCassandraKeyspace());
        cl = ConsistencyLevel.valueOf(Config.getCassandraConsistencyLevel());

        writeColumn = new Column();
        writeColumnFamily = new ColumnParent(Config.getCassandraColumnFamily());

        columnPath = new ColumnPath(Config.getCassandraColumnFamily());
        bytesRead = bytesWritten = readLatency = writeLatency = 0;
    }

    public void close() throws Exception {
        transport.close();
    }

    public byte[] get(String key) throws Exception {
        return get(key, false);
    }

    public void put(String key, byte[] value, long timestamp) throws Exception {
        put(key, value, timestamp, false);
    }

    public byte[] get(String key, boolean recordLat) throws Exception {
        long startTime = 0;
        if(recordLat)
            startTime = System.currentTimeMillis();
        columnPath.column = ByteBufferUtil.bytes(key);
        byte [] ret;
        try {
            Column column = client.get(ByteBufferUtil.bytes(key), columnPath, cl).getColumn();
            if(recordLat)
                System.out.println("RECORD GET "+key+" "+column.getTimestamp());
            ret = column.getValue();
        }
        catch (Exception NotFoundException) {
            if(recordLat)
                writeLatency += (System.currentTimeMillis()-startTime);
            return null;
        }

        bytesRead += ret.length;
        if(recordLat)
            readLatency += (System.currentTimeMillis()-startTime);
        return ret;
    }

    public void put(String key, byte[] value, long timestamp, boolean recordLat) throws Exception {
        long startTime = 0;
        if(recordLat)
            startTime = System.currentTimeMillis();
        writeColumn.setName(ByteBufferUtil.bytes(key));
        writeColumn.setValue(value);
        writeColumn.setTimestamp(timestamp);

        if(recordLat)
            System.out.println("RECORD PUT "+key+" "+timestamp);

        client.insert(ByteBufferUtil.bytes(key), writeColumnFamily, writeColumn, cl);
        bytesWritten += value.length + key.length();
        if(recordLat)
            writeLatency += (System.currentTimeMillis()-startTime);
    }

    public long getBytesWritten() {
        return bytesWritten;
    }

    public long getBytesRead() {
        return bytesRead;
    }

    public long getReadLatency() {
        return readLatency;
    }

    public long getWriteLatency() {
        return writeLatency;
    }

    public long getNumReads() {
        return -1;
    }

    public long getNumWrites() {
        return -1;
    }
}