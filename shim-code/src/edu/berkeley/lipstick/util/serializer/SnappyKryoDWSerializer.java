package edu.berkeley.lipstick.util.serializer;

import edu.berkeley.lipstick.util.DataWrapper;
import org.xerial.snappy.Snappy;

public class SnappyKryoDWSerializer implements IDWSerializer {

    private KryoDWSerializer pdw = new KryoDWSerializer();

    public byte[] toByteArray(DataWrapper input) throws Exception {
        return Snappy.compress(pdw.toByteArray(input));
    }

    public DataWrapper fromByteArray(String key, byte[] input) throws Exception {
        return pdw.fromByteArray(key, Snappy.uncompress(input));
    }
}