package edu.berkeley.lipstick.storage;

import java.util.concurrent.ConcurrentHashMap;

public class DummyStorage implements IStorage {
    class WriteRecord {
        private long timestamp;
        private byte[] value;

        public long getTimestamp() {
            return timestamp;
        }

        public byte[] getValue() {
            return value;
        }

        public WriteRecord(long timestamp, byte[] value) {
            this.timestamp = timestamp;
            this.value = value;
        }

    }

    private long bytesWritten = 0L;
    private long bytesRead = 0L;

    private ConcurrentHashMap<Object, WriteRecord> store;

    public DummyStorage()
    {
        store = new ConcurrentHashMap<Object, WriteRecord>();
    }

    public void open() throws Exception {}

    public void close() throws Exception {}



    public byte[] get(String key, boolean recordLat) throws Exception {
        WriteRecord ret = store.get(key);
        if(ret == null)
            return null;

        bytesRead += ret.getValue().length;

        return ret.getValue();
    }

    public byte[] get(String key) throws Exception {
        return get(key, false);
    }

    public void put(String key, byte[] value, long timestamp) throws Exception {
        put(key, value, timestamp, false);
    }

    public void put(String key, byte[] value, long timestamp, boolean recordLat) throws Exception {

        /*
        int replylen = 0;
        int replies = 0;
        int maxReply = -1;
        int zeros = 0;
        int totallens = 0;
        for(WriteRecord w : store.values()) {
            replies++;

            int curlen = w.getValue().length;

            totallens += curlen;

            if(curlen == 1)
                zeros++;

            if (curlen > maxReply)
                maxReply = curlen;

            replylen += curlen;
        }
        System.out.printf("Avg: %f, Convo: %f, Zeroes: %f, MaxConvo: %d\n", (double) totallens/replies, (double)replylen/(replies-zeros), (double) zeros/replies, maxReply);
        */


        bytesWritten += value.length + key.length();
        if(!store.containsKey(key) || timestamp > store.get(key).getTimestamp())
            store.put(key, new WriteRecord(timestamp, value));
    }

    public long getBytesWritten() {
        return bytesWritten;
    }

    public long getBytesRead() {
        return bytesRead;
    }

    public long getReadLatency() {
        return -1;
    }

    public long getWriteLatency() {
        return -1;
    }

    public long getNumReads() {
        return -1;
    }

    public long getNumWrites() {
        return -1;
    }
}
