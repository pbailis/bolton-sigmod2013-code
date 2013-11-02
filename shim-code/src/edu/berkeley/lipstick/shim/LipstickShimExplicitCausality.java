package edu.berkeley.lipstick.shim;

import edu.berkeley.lipstick.backend.IExplicitBackend;
import edu.berkeley.lipstick.config.Config;
import edu.berkeley.lipstick.util.DataWrapper;

import java.util.Set;

public class LipstickShimExplicitCausality {

    private IExplicitBackend backend;

    public LipstickShimExplicitCausality() throws Exception {
        backend = Config.getExplicitBackend();
    }

    public void open() throws Exception {
        backend.open();
    }

    public void close() throws Exception {
        backend.close();
    }

    public final DataWrapper get(String key) throws Exception {
        return backend.get(key);
    }

    public DataWrapper put_after(String key, Object value, final DataWrapper after) throws Exception {
        return backend.put_after(key, value, after);
    }

    public DataWrapper put_after(String key, Object value, final Set<DataWrapper> after) throws Exception {
        return backend.put_after(key, value, after);
    }

    public DataWrapper put_at_start(String key, Object value) throws Exception {
        return backend.put_at_start(key, value);
    }

}
