package edu.berkeley.lipstick.util.serializer;

import edu.berkeley.lipstick.util.DataWrapper;

public interface IDWSerializer {
    public byte[] toByteArray(DataWrapper input) throws Exception;
    public DataWrapper fromByteArray(String key, byte[] input) throws Exception;
}