package edu.berkeley.lipstick.backend;

import edu.berkeley.lipstick.config.Config;
import edu.berkeley.lipstick.localstore.ILocalStore;
import edu.berkeley.lipstick.storage.IStorage;
import edu.berkeley.lipstick.util.*;
import edu.berkeley.lipstick.util.serializer.IDWSerializer;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

public class AsynchronousReadExplicitBackend implements IExplicitBackend {
    IStorage storage;
    AtomicLong localSequenceNo;

    WriteRecorder localWriteRecorder;
    ILocalStore localStore;

    Iterable<DataWrapper> responseQueue;
    IDWSerializer DWserializer;
    AsynchronousResolver resolver;

    AtomicLong nullReads = new AtomicLong(0);

    Thread cleaner;

    String myPid;
    final int maxNumECDSreads;

    public AsynchronousReadExplicitBackend(IStorage storage) throws Exception {
        this.storage = storage;
        localWriteRecorder = new WriteRecorder();
        localStore = Config.getLocalStore();
        responseQueue = new ConcurrentLinkedQueue<DataWrapper>();
        DWserializer = Config.getDWSerializer();

        localSequenceNo = new AtomicLong(0);

        maxNumECDSreads = Config.getBackendMaxSyncECDSReads();

        resolver = new AsynchronousResolver(localStore,
                                            storage,
                                            DWserializer);

        myPid = Config.getProcessID();

        if(Config.doResolveInBackground()) {
            cleaner = new Thread(resolver);
            cleaner.setDaemon(true);
            cleaner.setPriority(Thread.MIN_PRIORITY);
            cleaner.start();
        }
    }

    public void open() throws Exception {
        storage.open();
    }
    public void close() throws Exception {
        resolver.close();
        System.out.println("memory: " + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) +
        "; byteswritten: " + Config.getBytesWritten() +
        "; bytesread: " + Config.getBytesRead() +
        "; storagetotalwritelatency: "+Config.getStorageWriteLatency()+
        "; storagetotalreadlatency: "+Config.getStorageReadLatency()+
        "; totalvisibilitytime: "+localStore.getTotalVisiblityLatency()+
        "; totalvisibilityresolvedreads: "+localStore.getNumResolvedReads()+
        "; clientnullreads: "+nullReads.get()+
        "; datastorereads: "+storage.getNumReads()+
        "; datastorewrites: "+storage.getNumWrites());
        storage.close();
    }

    public DataWrapper get(String key) throws Exception {

        if(maxNumECDSreads == 0) {
            DataWrapper ret = localStore.get(key);
            resolver.addKeyToCheck(key);

            if(ret == null)
                nullReads.incrementAndGet();

            return ret;
        }

        byte[] response = (byte [])storage.get(key, true);

        if(response != null && response.length != 0) {
            DataWrapper readResponse = Config.getDWSerializer().fromByteArray(key, response);

            if(resolver.checkSingleKey(readResponse, maxNumECDSreads-1))
                return readResponse;
            else
                readResponse.markReadFromStorage();
        }

        DataWrapper ret = localStore.get(key);

        if(ret == null)
            nullReads.incrementAndGet();

        return ret;
    }

    public final DataWrapper put_at_start(String key, Object value) throws Exception {
        return put_after(key, value, new HashSet<DataWrapper>());
    }

    public final DataWrapper put_after(String key, Object value, DataWrapper after) throws Exception {
        Set<DataWrapper> toPass = new HashSet<DataWrapper>();
        if(after != null) {
            toPass.add(after);
        }

        return put_after(key, value, toPass);
    }

    public final DataWrapper put_after(String key, Object value, Set<DataWrapper> after) throws Exception {
        long timestamp = System.currentTimeMillis();

        KeyDependencySet thisKDS = new KeyDependencySet(after);

        WriteClock thisWriteClock = new WriteClock();

        for(DataWrapper dep : after) {
            if(dep.getKey().matches(key)) {
                thisWriteClock.mergeClock(dep.getDependency(key).getClock());
            }
        }

        thisWriteClock.setValue(Config.getProcessID(), localSequenceNo.getAndIncrement());

        thisKDS.putDependency(key, new KeyDependency(thisWriteClock));

        DataWrapper toWrite = new DataWrapper(key,
                                              value,
                                              thisKDS,
                                              timestamp);

        storage.put(key, DWserializer.toByteArray(toWrite), timestamp, true);
        localStore.put(key, toWrite);
        return toWrite;
    }
}