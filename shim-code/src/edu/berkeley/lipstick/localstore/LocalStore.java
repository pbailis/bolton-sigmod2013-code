package edu.berkeley.lipstick.localstore;

/* Shims need to keep copies of data items around as well */

import edu.berkeley.lipstick.util.DataWrapper;

import java.util.HashMap;
import java.util.Map;

public class LocalStore {

    Map<String, DataWrapper> store;

    public LocalStore() {
        store = new HashMap<String, DataWrapper>();
    }

    public DataWrapper get(String key) {
        return store.get(key);
    }

    public void put(String key, DataWrapper value)throws Exception {
        assert(!value.getWriteClock().happensBefore(store.get(key).getWriteClock()));
        store.put(key, value);
    }

    public void close() throws Exception {}
}