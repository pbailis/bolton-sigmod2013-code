package edu.berkeley.lipstick.localstore;

import edu.berkeley.lipstick.util.WriteClock;

public class LocalStoredWrite {
    Object value;
    long timestamp;
    String writer;
    WriteClock clock;

    public LocalStoredWrite(Object value, long timestamp, String writer, WriteClock clock) {
        this.value = value;
        this.timestamp = timestamp;
        this.writer = writer;
        this.clock = clock;
    }

    public Object getValue() {
        return value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getWriter() {
        return writer;
    }

    public WriteClock getClock() {
        return clock;
    }
}