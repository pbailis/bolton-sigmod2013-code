package edu.berkeley.lipstick.backend;

import edu.berkeley.lipstick.config.Config;
import edu.berkeley.lipstick.localstore.ILocalStore;
import edu.berkeley.lipstick.storage.IStorage;
import edu.berkeley.lipstick.util.*;
import edu.berkeley.lipstick.util.serializer.IDWSerializer;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class AsynchronousResolver implements Runnable {

    VersionApplier applier;
    IStorage storage;
    ConcurrentHashMap<String, String> keysToCheck = new ConcurrentHashMap<String, String>();
    IDWSerializer serializer;
    boolean closed = false;
    final long maxKeysToCheck;

    public AsynchronousResolver(ILocalStore localstore,
                                IStorage storage,
                                IDWSerializer serializer) throws Exception {

        applier = new VersionApplier(localstore, storage, this);
        this.storage = storage;
        this.serializer = serializer;
        maxKeysToCheck = Config.getMaxKeysToCheck();
    }

    public void close() {
        closed = true;
    }

    public void addKeyToCheck(String key) {
        if(keysToCheck.size() < maxKeysToCheck)
            keysToCheck.put(key, "");
    }

    public boolean checkSingleKey(DataWrapper wrapper, int numEDCSreads) throws Exception {
        if(applier.checkSingleKey(wrapper, numEDCSreads))
            return true;

        applier.addToCheck(wrapper);
        return false;
    }

    public void run() {
        while(true) {
            if(closed)
                break;

            try {
                Thread.sleep(Config.getAsyncSleepLength());

                for(String key : keysToCheck.keySet()) {
                    byte[] ret = storage.get(key);
                    if(ret != null) {
                        applier.addToCheck(serializer.fromByteArray(key, ret));
                    }
                }

                keysToCheck.clear();

                applier.applyAllPossible();
            }
            catch(Exception e) {
                if(closed)
                    break;
                System.out.println(e.getClass());
                System.out.println(e.getCause());
                System.out.println(e.getMessage());
                e.printStackTrace();
            }
        }
    }
}