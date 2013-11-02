package edu.berkeley.lipstick.util;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class WriteRecorder {

    private Map<Object, Long> writeNumbers;

    public WriteRecorder()
    {
        writeNumbers = new ConcurrentHashMap<Object, Long> ();
    }

    public long getWriteNumber(Object key)
    {
        if(!writeNumbers.containsKey(key))
        {
            writeNumbers.put(key, 0L);
        }

        return writeNumbers.put(key, writeNumbers.get(key)+1)+1;
    }
}