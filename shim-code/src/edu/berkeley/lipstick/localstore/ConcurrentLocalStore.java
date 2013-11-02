package edu.berkeley.lipstick.localstore;

/* Shims need to keep copies of data items around as well */

import edu.berkeley.lipstick.util.DataWrapper;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class ConcurrentLocalStore implements ILocalStore {

    ConcurrentHashMap<String, DataWrapper> store;

    AtomicLong resolvedReads = new AtomicLong(0);
    AtomicLong totalVisLatency = new AtomicLong(0);

    public ConcurrentLocalStore()
    {
        store = new ConcurrentHashMap<String, DataWrapper>();
    }

    public DataWrapper get(String key) throws Exception {
        return store.get(key);
    }

    public void put(String key, DataWrapper value) throws Exception {
        assert(!value.getWriteClock().happensBefore(store.get(key).getWriteClock()));

        /*
        int replylen = 0;
        int replies = 0;
        int maxReply = -1;
        int zeros = 0;
        int totallens = 0;
        int kdssize = 0;
        for(DataWrapper w : store.values()) {
            replies++;

            int curlen = ByteBuffer.wrap((byte [])w.getValue()).getInt(4);

            totallens += curlen;

            if(curlen == 1)
                zeros++;

            if (curlen > maxReply)
                maxReply = curlen;

            replylen += curlen;
            kdssize += w.getKeyDependencySet().getKeys().size();
        }
        System.out.printf("%d, Avg: %f, Convo: %f, Zeroes: %f, MaxConvo: %d, KDS:%f\n", store.size(), (double) totallens/replies, (double)replylen/(replies-zeros), (double) zeros/replies, maxReply, (double) kdssize/replies);
        */

        if(value.hasReadFromStorageTime()) {
            long latency = System.currentTimeMillis()-value.getReadFromStorageTime();
            totalVisLatency.addAndGet(latency);
            resolvedReads.incrementAndGet();
        }

        DataWrapper current = store.get(key);
        if(current == null || current.getTimestamp() < value.getTimestamp())
            store.put(key, value);
    }

    public void close() throws Exception {}

    public long getNumResolvedReads() {
        return resolvedReads.get();
    }

    public long getTotalVisiblityLatency() {
        return totalVisLatency.get();
    }
}