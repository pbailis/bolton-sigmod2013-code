package edu.berkeley.lipstick.storage;

import java.io.BufferedWriter;
import java.io.FileWriter;

public class PrintingDevNullStorage implements IStorage {

    long bytesRead = 0;
    long bytesWritten = 0;

    FileWriter writer;
    BufferedWriter bw;

    public PrintingDevNullStorage() throws Exception {
        writer = new FileWriter("/tmp/sizes.out");
        bw = new BufferedWriter(writer);
    }

    public void open() throws Exception {}
    public void close() throws Exception { bw.flush(); bw.close(); writer.close(); }

    public byte[] get(String key, boolean recordLat) throws Exception { return null; }
    public byte[] get(String key) throws Exception { return null; }
    public void put(String key, byte[] value, long timestamp) throws Exception { bw.write(String.format("%d\n", value.length)); }
    public void put(String key, byte[] value, long timestamp, boolean recordLat) throws Exception { bw.write(String.format("%d\n", value.length)); }

    public long getBytesWritten() {
        return bytesWritten;
    }
    public long getBytesRead() {
        return bytesRead;
    }

    public long getReadLatency() {
        return 0;
    }

    public long getWriteLatency() {
        return 0;
    }

    public long getNumReads() {
        return -1;
    }

    public long getNumWrites() {
        return -1;
    }
}