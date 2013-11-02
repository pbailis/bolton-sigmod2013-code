package edu.berkeley.lipstick.util;

import java.io.Serializable;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class WriteClock implements Serializable {

    ConcurrentHashMap<String, AtomicLong> clock;

    public static int IS_EQUAL = -1;
    public static int HAPPENS_BEFORE = -2;
    public static int HAPPENS_AFTER = -3;
    public static int IS_CONCURRENT = -4;

    public WriteClock() {
        clock = new ConcurrentHashMap<String, AtomicLong>();
    }

    public WriteClock(WriteClock wc) {
        if(wc != null) {
            clock = new ConcurrentHashMap<String, AtomicLong>();
            for(String key : wc.getWriters())
                clock.put(key, new AtomicLong(wc.getValue(key)));
        }
        else {
            clock = new ConcurrentHashMap<String, AtomicLong>();
        }
    }

    public final long getValue(String which) {
        if(clock.containsKey(which))
            return clock.get(which).get();
        else
            return 0;
    }

    public void incrementValue(String which) {
        if(! clock.containsKey(which))
            clock.put(which, new AtomicLong(1));
        else
            clock.get(which).incrementAndGet();
    }

    public void setValue(String which, long value) {
        if(clock.get(which) == null)
            clock.put(which, new AtomicLong(value));
        else
            clock.get(which).set(value);
    }

    public final Set<String> getWriters() {
        return clock.keySet();
    }

    public void mergeClock(WriteClock otherClock) {
        Iterator<String> otherIt = otherClock.getWriters().iterator();

        while(otherIt.hasNext())
        {
            String otherWriter = otherIt.next();
            if(clock.containsKey(otherWriter)) {
                clock.put(otherWriter, new AtomicLong(Math.max(this.getValue(otherWriter), otherClock.getValue(otherWriter))));
            }
            else
            {
                clock.put(otherWriter, new AtomicLong(otherClock.getValue(otherWriter)));
            }
        }
    }

    public final int compareToClock(WriteClock otherClock) {
        boolean earlier = false;
        boolean later = false;
        Set<String> toCheck = new HashSet<String>();
        toCheck.addAll(otherClock.getWriters());
        toCheck.addAll(getWriters());

        for(String writer : toCheck) {
            long ourWrite = getValue(writer);
            long theirWrite = otherClock.getValue(writer);

            if(ourWrite < theirWrite)
                earlier = true;
            else if(ourWrite > theirWrite)
                later = true;
        }

         if(earlier && !later)
            return HAPPENS_BEFORE;
         else if(!earlier && later)
                return HAPPENS_AFTER;
         else if(earlier && later)
            return IS_CONCURRENT;
         else
            return IS_EQUAL;
    }

    public final boolean happensBefore(WriteClock otherClock) {
        return compareToClock(otherClock) == HAPPENS_BEFORE;
    }

    public final boolean equals(WriteClock otherClock) {
        return compareToClock(otherClock) == IS_EQUAL;
    }
}
