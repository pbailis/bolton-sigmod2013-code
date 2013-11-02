package edu.berkeley.lipstick.backend;

import edu.berkeley.lipstick.config.Config;
import edu.berkeley.lipstick.localstore.ILocalStore;
import edu.berkeley.lipstick.storage.IStorage;
import edu.berkeley.lipstick.util.*;
import edu.berkeley.lipstick.util.serializer.IDWSerializer;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

public class PrintingPutOnlyExplicitBackend implements IExplicitBackend {

    AtomicLong localSequenceNo;

    String myPid;
    final int maxNumECDSreads;
    IDWSerializer DWserializer;

    FileWriter writer;
    BufferedWriter bw;

    public PrintingPutOnlyExplicitBackend(IStorage storage) throws Exception {

        localSequenceNo = new AtomicLong(0);
        writer = new FileWriter("/tmp/sizes.out");
        bw = new BufferedWriter(writer);

        DWserializer = Config.getDWSerializer();

        maxNumECDSreads = Config.getBackendMaxSyncECDSReads();

        myPid = Config.getProcessID();
    }

    public void open() throws Exception {
    }
    public void close() throws Exception {
        bw.close();
    }

    public DataWrapper get(String key) throws Exception {
       return null;
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


        bw.write(String.format("%d %d\n", DWserializer.toByteArray(toWrite).length, thisKDS.getDependencies().size()));

        return toWrite;
    }
}