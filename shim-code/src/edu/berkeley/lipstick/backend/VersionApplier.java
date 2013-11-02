package edu.berkeley.lipstick.backend;

import edu.berkeley.lipstick.config.Config;
import edu.berkeley.lipstick.localstore.ILocalStore;
import edu.berkeley.lipstick.storage.IStorage;
import edu.berkeley.lipstick.util.*;
import edu.berkeley.lipstick.util.serializer.IDWSerializer;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class VersionApplier {
    ConcurrentHashMap<String, DataWrapper> bufferedWrites = new ConcurrentHashMap<String, DataWrapper>();

    ILocalStore localStore;
    IStorage remoteStore;
    IDWSerializer serializer;
    AsynchronousResolver resolver;
    final int maxBufferedWrites;

    public VersionApplier(ILocalStore localStore,
                          IStorage remoteStore,
                          AsynchronousResolver resolver) throws Exception {
        this.localStore = localStore;
        this.remoteStore = remoteStore;
        serializer = Config.getDWSerializer();
        this.resolver = resolver;
        maxBufferedWrites = Config.getMaxBufferedWrites();
    }

    private void bufferWrite(DataWrapper wrapper) {
        if(bufferedWrites.size() < maxBufferedWrites)
            bufferedWrites.put(wrapper.getKey(), wrapper);
    }

    private void unbufferWrite(DataWrapper wrapper) {
        DataWrapper curWrapper = bufferedWrites.get(wrapper.getKey());
        if(curWrapper == wrapper)
            bufferedWrites.remove(wrapper.getKey());
    }

    private DataWrapper getBufferedWrite(String key) {
        return bufferedWrites.get(key);
    }

    public void addToCheck(DataWrapper newWrapper) throws Exception {
        bufferWrite(newWrapper);
    }

    public boolean checkSingleKey(DataWrapper checkWrapper, int numECDSchecks) throws Exception {
        Vector<DataWrapper> keysToApply = new Vector<DataWrapper>();

        if(!attemptToCover(checkWrapper, keysToApply, numECDSchecks))
            return false;

        if(keysToApply.size() == 0)
            return true;

        for(DataWrapper wrapper : keysToApply) {
            localStore.put(wrapper.getKey(), wrapper);

            unbufferWrite(wrapper);
        }

        return true;
    }

    private void addToConsider(String key, WriteClock wc, Map<String, List<WriteClock>> consider) {
        List<WriteClock> wrappers = consider.get(key);

        if(wrappers == null) {
            wrappers = new ArrayList<WriteClock>();
            consider.put(key, wrappers);
        }

        wrappers.add(wc);
    }

    private boolean attemptToCover(DataWrapper toCheck, List<DataWrapper> keysToApply, final int numECDSreads) throws Exception {
        Map<String, List<WriteClock>> toConsider = new HashMap<String, List<WriteClock>>();
        addToConsider(toCheck.getKey(), toCheck.getWriteClock(), toConsider);
        return attemptToCover(toCheck, keysToApply, toConsider, numECDSreads);
    }

    private boolean attemptToCover(DataWrapper toCheck,
                                   List<DataWrapper> keysToApply,
                                   Map<String, List<WriteClock>> extraWritesToConsider,
                                   final int remainingECDSdepth) throws Exception {

        // see if we've already applied this one
        DataWrapper localWrapper = localStore.get(toCheck.getKey());
        if(localWrapper != null) {
            int causality = localWrapper.getWriteClock().compareToClock(toCheck.getWriteClock());
            if(causality == WriteClock.HAPPENS_AFTER || causality == WriteClock.IS_EQUAL)
                return true;
        }

        for(String keyDep : toCheck.getKeyDependencySet().getKeys()) {
            if(keyDep.equals(toCheck.getKey()))
                continue;

            DataWrapper storedWrapper = localStore.get(keyDep);
            WriteClock depClock = toCheck.getDependency(keyDep).getClock();

            //if we've already applied this write or a write that will overwrite this write, we're good
            if(storedWrapper != null
                    && storedWrapper.getWriteClock().compareToClock(depClock) != WriteClock.HAPPENS_BEFORE) {
                addToConsider(keyDep, depClock, extraWritesToConsider);
                continue;
            }

            // if we've already checked that we have a suitable cover for this value
            if(extraWritesToConsider.containsKey(keyDep)) {
                boolean found = false;
                for(WriteClock alreadyApplied : extraWritesToConsider.get(keyDep)) {
                    if(alreadyApplied.compareToClock(depClock) != WriteClock.HAPPENS_BEFORE) {
                        //don't include this in to consider since we're already using another
                        //dependency (alreadyApplied!)
                        found = true;
                        break;
                    }
                }

                if(found)
                    continue;
            }

            if(remainingECDSdepth <= 0) {
                resolver.addKeyToCheck(keyDep);
                return false;
            }

            //now check any buffered writes
            DataWrapper bufferedWrapper = getBufferedWrite(keyDep);
            if(bufferedWrapper != null) {
                int causality = bufferedWrapper.getWriteClock().compareToClock(depClock);
                if(causality == WriteClock.IS_EQUAL ||
                        ((causality == WriteClock.HAPPENS_AFTER || causality == WriteClock.IS_CONCURRENT)
                          && (attemptToCover(bufferedWrapper, keysToApply, extraWritesToConsider, remainingECDSdepth)))) {
                    addToConsider(keyDep, depClock, extraWritesToConsider);
                    keysToApply.add(bufferedWrapper);
                    continue;
                }
            }

            //now try to read from the underlying store...
            byte[] readWriteBytes = remoteStore.get(keyDep);
            if(readWriteBytes == null)
                return false;

            DataWrapper newlyReadWrite = serializer.fromByteArray(keyDep, readWriteBytes);

            int causality = newlyReadWrite.getWriteClock().compareToClock(depClock);
            if(causality == WriteClock.IS_EQUAL ||
                    ((causality == WriteClock.HAPPENS_AFTER || causality == WriteClock.IS_CONCURRENT)
                      && (attemptToCover(newlyReadWrite, keysToApply, extraWritesToConsider, remainingECDSdepth-1)))) {
                addToConsider(keyDep, depClock, extraWritesToConsider);
                keysToApply.add(newlyReadWrite);
                continue;
            }
            else
                addToCheck(newlyReadWrite);
            return false;
        }

        keysToApply.add(toCheck);
        return true;
    }

    public void applyAllPossible() throws Exception {
        while(true) {
            boolean changed = false;

            ArrayList<DataWrapper> writesToConsider = new ArrayList<DataWrapper>(bufferedWrites.values());

            for(DataWrapper frontier : writesToConsider) {
                Vector<DataWrapper> keysToApply = new Vector<DataWrapper>();

                boolean covered = attemptToCover(frontier, keysToApply, Integer.MAX_VALUE);

                if(!covered)
                    continue;

                for(DataWrapper wrapper : keysToApply) {
                    localStore.put(wrapper.getKey(), wrapper);

                    unbufferWrite(wrapper);
                    changed = true;
                }

                if(changed)
                    break;
            }

            if(!changed)
                break;
        }
    }
}