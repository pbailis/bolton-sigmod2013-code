package edu.berkeley.lipstick.simpleserver;

import com.sun.tools.javac.util.Pair;

import org.apache.cassandra.thrift.NotFoundException;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SimpleServerServiceImpl implements SimpleServerService.Iface {

    private Map<String, Pair<Long, ByteBuffer>> storage;

    public SimpleServerServiceImpl()
    {
        storage = new ConcurrentHashMap<String, Pair<Long, ByteBuffer>>();
    }

    public ByteBuffer get(String key) throws TException{
        Pair<Long, ByteBuffer> ret = storage.get(key);
        if(ret == null)
            throw new TTransportException();
        return storage.get(key).snd;

    }

    public void put(String key, ByteBuffer value, long timestamp) throws TException {
        if(!storage.containsKey(key) || timestamp > storage.get(key).fst)
            storage.put(key, new Pair<Long, ByteBuffer>(timestamp, value));
    }
}