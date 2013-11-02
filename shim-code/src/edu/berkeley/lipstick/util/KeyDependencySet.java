package edu.berkeley.lipstick.util;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class KeyDependencySet implements Serializable {
    private ConcurrentHashMap<String, KeyDependency> deps;

    public KeyDependencySet() {
        deps = new ConcurrentHashMap<String, KeyDependency> ();
    }

    public final Set<String> getKeys() {
        return deps.keySet();
    }

    public KeyDependencySet putDependency(String key, KeyDependency newDep) {
        deps.put(key, newDep);
        return this;
    }

    public KeyDependencySet(Iterable<DataWrapper> dws) {
        this();
        for(DataWrapper dw : dws) {
            for(String depKey : dw.getKeyDependencySet().getKeys())
            {
                KeyDependency dep = dw.getKeyDependencySet().getDependency(depKey);

                if(deps.containsKey(depKey))
                    deps.get(depKey).getClock().mergeClock(dep.getClock());
                else
                    deps.put(depKey, new KeyDependency(dep));
            }
        }
    }

    public final KeyDependency getDependency(Object key) {
        return deps.get(key);
    }

    public final Collection<KeyDependency> getDependencies() {
        return deps.values();
    }
}
