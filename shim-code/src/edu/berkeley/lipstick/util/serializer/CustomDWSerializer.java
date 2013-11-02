package edu.berkeley.lipstick.util.serializer;

import edu.berkeley.lipstick.util.DataWrapper;
import edu.berkeley.lipstick.util.KeyDependency;
import edu.berkeley.lipstick.util.KeyDependencySet;
import edu.berkeley.lipstick.util.WriteClock;

import java.io.*;
import java.nio.ByteBuffer;

public class CustomDWSerializer implements IDWSerializer {

    private static final ThreadLocal<ByteBuffer> threadBuf = new ThreadLocal<ByteBuffer>(){
        @Override
        protected ByteBuffer initialValue()
        {
            return ByteBuffer.allocate(4096);
        }
    };

    private void putString(String s) {
        byte[] sBytes = s.getBytes();
        threadBuf.get().putShort((short) sBytes.length);
        threadBuf.get().put(sBytes);
    }

    private void putClock(WriteClock wc) {
        threadBuf.get().putShort((short) wc.getWriters().size());
        for(String writer : wc.getWriters()) {
            putString(writer);
            threadBuf.get().putInt((int) wc.getValue(writer));
        }
    }

    private void putKeyDependency(String key, KeyDependency kd) {
        putString(key);
        putClock(kd.getClock());
    }

    public byte[] toByteArray(DataWrapper input) throws Exception {
        ByteBuffer buf = threadBuf.get();

        buf.clear();

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = new ObjectOutputStream(bos);
        out.writeObject(input.getValue());
        out.close();

        buf.putShort((short)bos.size());
        buf.put(bos.toByteArray());

        buf.putLong(input.getTimestamp());

        buf.putShort((short) input.getKeyDependencySet().getDependencies().size());

        for(String kdString : input.getKeyDependencySet().getKeys()) {
            putKeyDependency(kdString, input.getDependency(kdString));
        }

        byte[] b = new byte[buf.position()];
        buf.rewind();
        buf.get(b);
        return b;
    }

    private String getString(ByteBuffer buf) {
        short len = buf.getShort();
        byte[] strBytes = new byte[len];
        buf.get(strBytes);
        return new String(strBytes);
    }

    private KeyDependency getKeyDependency(ByteBuffer buf) {
        WriteClock wc = new WriteClock();
        short entries = buf.getShort();
        for(int i = 0; i < entries; ++i) {
            String writer = getString(buf);
            int value = buf.getInt();
            wc.setValue(writer,  value);
        }

        return new KeyDependency(wc);
    }

    public DataWrapper fromByteArray(String key, byte[] input) throws Exception {
        ByteBuffer buf = ByteBuffer.wrap(input);

        short valueLen = buf.getShort();
        byte[] valueBytes = new byte[valueLen];
        buf.get(valueBytes);

        ByteArrayInputStream bis = new ByteArrayInputStream(valueBytes);
        ObjectInput in = new ObjectInputStream(bis);
        Object o = in.readObject();
        in.close();

        long timestamp = buf.getLong();

        KeyDependencySet kds = new KeyDependencySet();
        DataWrapper ret = new DataWrapper(key, o, kds, timestamp);

        short kdsSize = buf.getShort();
        for(short i = 0; i < kdsSize; ++i) {
            String depKey = getString(buf);
            kds.putDependency(depKey, getKeyDependency(buf));
        }

        return ret;
    }
}