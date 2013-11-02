package edu.berkeley.lipstick.storage;

import edu.berkeley.lipstick.config.Config;
import edu.berkeley.lipstick.simpleserver.SimpleServerService;

import java.nio.ByteBuffer;

import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransportException;


public class SimpleServerStorage {

    SimpleServerService.Client client;
    TTransport transport;

    long bytesWritten = 0L;
    long bytesRead = 0L;

    public void open() throws Exception {
        transport = new TSocket(Config.getSimpleBackendHost(), Config.getSimpleBackendPort());
        client = new SimpleServerService.Client(new TBinaryProtocol(transport));
        transport.open();
    }

    public void close() throws Exception {
        transport.close();
    }

    public byte[] get(String key) throws Exception {
        try {
            byte [] ret = client.get(key).array();
            bytesRead += ret.length;
            return ret;
        } catch (TTransportException e) {
            return null;
        }
    }

    public void put(String key, byte[] value, long timestamp) throws Exception {
        bytesWritten += value.length + key.length();
        client.put(key, ByteBuffer.wrap(value), timestamp);
    }

    public long getBytesWritten() {
        return bytesWritten;
    }

    public long getBytesRead() {
        return bytesRead;
    }

    public long getReadLatency() {
        return -1;
    }

    public long getWriteLatency() {
        return -1;
    }

}