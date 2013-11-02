package edu.berkeley.lipstick.storage;

public class PrintingPooledCassandraStorage implements IStorage {
    PooledStorage ps;

    public PrintingPooledCassandraStorage() {
        ps = new PooledStorage(PrintingCassandraStorage.class);
    }

    public void open() throws Exception {
       ps.open();
    }

    public void close() throws Exception {
        ps.close();
    }

    public byte[] get(String key) throws Exception {
        return ps.get(key);
    }

    public byte[] get(String key, boolean returnLat) throws Exception {
        return ps.get(key, returnLat);
    }

    public void put(String key, byte[] value, long timestamp) throws Exception {
        ps.put(key, value, timestamp);
    }

    public void put(String key, byte[] value, long timestamp, boolean returnLat) throws Exception {
        ps.put(key, value, timestamp, returnLat);
    }

    public long getBytesWritten() {
        return ps.getBytesWritten();
    }

    public long getBytesRead() {
        return ps.getBytesRead();
    }

    public long getReadLatency() {
        return ps.getReadLatency();
    }

    public long getWriteLatency() {
        return ps.getWriteLatency();
    }

    public long getNumReads() {
        return ps.getNumReads();
    }

    public long getNumWrites() {
        return ps.getNumReads();
    }
}