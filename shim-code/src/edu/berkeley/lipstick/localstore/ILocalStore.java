package edu.berkeley.lipstick.localstore;

import edu.berkeley.lipstick.util.DataWrapper;

public interface ILocalStore {
    public DataWrapper get(String key) throws Exception;
    public void put(String key, DataWrapper value) throws Exception;
    public void close() throws Exception;
    public long getNumResolvedReads();
    public long getTotalVisiblityLatency();
}