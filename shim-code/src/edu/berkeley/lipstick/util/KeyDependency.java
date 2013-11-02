package edu.berkeley.lipstick.util;

import java.io.Serializable;

public class KeyDependency implements Serializable {
    private WriteClock wc;

    private KeyDependency() {}

    public KeyDependency(WriteClock wc) {
        this.wc = wc;
    }

    public KeyDependency(KeyDependency kd) {
        this.wc = new WriteClock(kd.getClock());
    }

    public final WriteClock getClock() {
        return this.wc;
    }
}