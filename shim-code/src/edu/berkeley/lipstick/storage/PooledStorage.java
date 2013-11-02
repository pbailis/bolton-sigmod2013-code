package edu.berkeley.lipstick.storage;

import java.io.InputStream;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class PooledStorage implements IStorage {
    AtomicInteger csCount;
    AtomicLong numReads;
    AtomicLong numWrites;
    Class storageClass;
    ConcurrentLinkedQueue<IStorage> pool;
    ConcurrentLinkedQueue<IStorage> allStorage;

    boolean open;

    public PooledStorage (Class storageClass) {
        this.storageClass = storageClass;
    }

    public void open() throws Exception {
        open = true;
        csCount = new AtomicInteger(0);
        numReads = new AtomicLong(0);
        numWrites = new AtomicLong(0);
        pool = new ConcurrentLinkedQueue<IStorage>();
        allStorage = new ConcurrentLinkedQueue<IStorage>();
    }

    IStorage getStorage() throws Exception {
        IStorage ret = pool.poll();

        if(ret == null) {
            csCount.incrementAndGet();
            ret = (IStorage) (storageClass.newInstance());
            ret.open();
            allStorage.add(ret);
        }

        return ret;
    }

    void returnStorage(IStorage cs) {
        pool.add(cs);
    }

    public void close() throws Exception {
        open = false;
        int closed = 0;
        // todo: this really isn't safe...
        while(closed < csCount.get()) {
            IStorage cs = pool.poll();
            if(cs != null) {
                cs.close();
                closed++;
            }
        }
    }

    public byte[] get(String key) throws Exception {
        return get(key, false);
    }

    public void put(String key, byte[] value, long timestamp) throws Exception {
        put(key, value, timestamp, false);
    }

    public byte[] get(String key, boolean recordLat) throws Exception {
        if(!open)
            throw new Exception("pools closed");

        numReads.incrementAndGet();

        IStorage cs = getStorage();
        byte[] ret = cs.get(key, recordLat);
        returnStorage(cs);
        return ret;
    }

    public void put(String key, byte[] value, long timestamp, boolean recordLat) throws Exception {
        if(!open)
            throw new Exception("pools closed");

        numWrites.incrementAndGet();

        IStorage cs = getStorage();
        cs.put(key, value, timestamp, recordLat);
        returnStorage(cs);
    }

    public long getBytesWritten() {
        long ret = 0;
        for(IStorage cs : allStorage) {
            ret += cs.getBytesWritten();
        }

        return ret;
    }

    public long getBytesRead() {
        long ret = 0;
        for(IStorage cs : allStorage) {
            ret += cs.getBytesRead();
        }

        return ret;
    }

    public long getReadLatency() {
        long ret = 0;
        for(IStorage cs : allStorage) {
            ret += cs.getReadLatency();
        }

        return ret;
    }

    public long getWriteLatency() {
        long ret = 0;
        for(IStorage cs : allStorage) {
            ret += cs.getWriteLatency();
        }

        return ret;
    }

        public long getNumReads() {
            return numReads.get();
    }

    public long getNumWrites() {
        return numWrites.get();
    }
}