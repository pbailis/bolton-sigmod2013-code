package edu.berkeley.lipstick.backend;

/*
    Used to do PUT/GET to underlying store without actually storing metadata or performing checks...
    For testing against eventually consistent configurations.
 */

import edu.berkeley.lipstick.storage.IStorage;
import edu.berkeley.lipstick.util.DataWrapper;
import edu.berkeley.lipstick.util.KeyDependencySet;

import java.util.Set;

public class ECExplicitBackend implements IExplicitBackend {
    IStorage storage;

    public ECExplicitBackend(IStorage storage) throws Exception {
        this.storage = storage;
    }

    public void open() throws Exception {
        storage.open();
    }
    public void close() throws Exception {
        storage.close();
    }

    public DataWrapper get(String key) throws Exception  {
        Object toWrap = storage.get(key);

        if(toWrap == null)
            return null;

        return new DataWrapper(key, toWrap, new KeyDependencySet(), -1);
    }
    public DataWrapper put_at_start(String key, Object value) throws Exception {
        dummy_put(key, value);
        return new DataWrapper(key, value, new KeyDependencySet(), -1);
    }

    public DataWrapper put_after(String key, Object value, DataWrapper after) throws Exception {
        dummy_put(key, value);
        return new DataWrapper(key, value, new KeyDependencySet(), -1);
    }

    public DataWrapper put_after(String key, Object value, Set<DataWrapper> after) throws Exception {
        dummy_put(key, value);
        return new DataWrapper(key, value, new KeyDependencySet(), -1);
    }

    private void dummy_put(String key, Object value) throws Exception {
        storage.put(key, (byte [])value, System.currentTimeMillis(), true);
    }

    public void put(String key, Object value) throws Exception {
        dummy_put(key, value);
    }
}