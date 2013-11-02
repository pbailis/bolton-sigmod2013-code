package edu.berkeley.lipstick.storage;

public interface IStorage {
    public void open() throws Exception;
    public void close() throws Exception;

    public byte[] get(String key, boolean recordLat) throws Exception;
    public byte[] get(String key) throws Exception;
    public void put(String key, byte[] value, long timestamp) throws Exception;
    public void put(String key, byte[] value, long timestamp, boolean recordLat) throws Exception;


    public long getNumReads();
    public long getNumWrites();

    public long getBytesWritten();
    public long getBytesRead();

    public long getReadLatency();
    public long getWriteLatency();
}
