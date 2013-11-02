package edu.berkeley.lipstick.backend;

import edu.berkeley.lipstick.util.DataWrapper;

import java.util.Set;

public interface IExplicitBackend {
    public void open() throws Exception;
    public void close() throws Exception;

    public DataWrapper get(String key) throws Exception;
    public DataWrapper put_at_start(String key, Object value) throws Exception;
    public DataWrapper put_after(String key, Object value, DataWrapper after) throws Exception;
    public DataWrapper put_after(String key, Object value, Set<DataWrapper> after) throws Exception;
}