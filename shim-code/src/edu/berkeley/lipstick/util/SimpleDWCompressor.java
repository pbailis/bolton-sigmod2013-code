package edu.berkeley.lipstick.util;

import edu.berkeley.lipstick.util.serializer.IDWSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class SimpleDWCompressor implements IDWSerializer {

    public byte[] toByteArray(DataWrapper input) throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        GZIPOutputStream df = new GZIPOutputStream(bos);
        ObjectOutput out = new ObjectOutputStream(df);
        out.writeObject(input);
        out.close();
        return bos.toByteArray();
    }

    public DataWrapper fromByteArray(String key, byte[] input) throws Exception {
        ByteArrayInputStream bis = new ByteArrayInputStream(input);
        GZIPInputStream df = new GZIPInputStream(bis);
        ObjectInput in = new ObjectInputStream(df);
        Object o = in.readObject();
        in.close();
        return (DataWrapper) o;
    }
}