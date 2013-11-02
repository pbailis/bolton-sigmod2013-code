package edu.berkeley.lipstick.util.serializer;


import edu.berkeley.lipstick.util.DataWrapper;

public class GzipProtoDWSerializer implements IDWSerializer {

    private ProtoDWSerializer pdw = new ProtoDWSerializer();

    public byte[] toByteArray(DataWrapper input) throws Exception {
        return GzipCompressor.compress(pdw.toByteArray(input));
    }

    public DataWrapper fromByteArray(String key, byte[] input) throws Exception {
        return pdw.fromByteArray(key, GzipCompressor.decompress(input));
   }
}