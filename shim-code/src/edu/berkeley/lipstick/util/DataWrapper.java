package edu.berkeley.lipstick.util;

import java.io.*;
import java.util.zip.*;

public class DataWrapper implements Serializable {
    private String key;
    private Object value;
    private KeyDependencySet kds;
    private long storageReadTime = -1;
    private boolean buffered = false;
    private long timestamp;

    public Object getValue() {
        return value;
    }

    public String getKey() {
        return key;
    }

    public KeyDependencySet getKeyDependencySet() {
        return kds;
    }

    private DataWrapper() {}

    public DataWrapper(String key, long timestamp) {
        this.key = key;
        this.value = null;
        this.kds = null;
        this.timestamp = timestamp;
        this.storageReadTime = System.currentTimeMillis();
    }

    public DataWrapper(String key, Object value, KeyDependencySet kds, long timestamp)
    {
        this.key = key;
        this.value = value;
        this.kds = kds;
        this.timestamp = timestamp;
        this.storageReadTime = System.currentTimeMillis();
    }

    public KeyDependency getDependency(String key) {
        return kds.getDependency(key);
    }

    public void markReadFromStorage() {
        buffered = true;
    }

    public long getReadFromStorageTime() {
        return storageReadTime;
    }

    public boolean hasReadFromStorageTime() {
        return buffered;
    }

    public final long getTimestamp() {
        return timestamp;
    }

    public final int compareClock(DataWrapper wrapper) {
        assert(wrapper.getKey().matches(this.key));

        return wrapper.getDependency(this.key).getClock().compareToClock(this.getDependency(this.key).getClock());
    }

    public WriteClock getWriteClock() {
        return getDependency(this.key).getClock();
    }

}