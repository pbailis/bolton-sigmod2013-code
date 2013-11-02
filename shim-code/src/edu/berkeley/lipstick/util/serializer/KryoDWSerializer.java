package edu.berkeley.lipstick.util.serializer;


import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import edu.berkeley.lipstick.util.DataWrapper;

import java.io.ByteArrayOutputStream;

public class KryoDWSerializer implements IDWSerializer {

    private static final ThreadLocal<Kryo> threadKryo = new ThreadLocal<Kryo>(){
        @Override
        protected Kryo initialValue()
        {
            Kryo ret = new Kryo();
            ret.register(DataWrapper.class);
            return ret;
        }
    };

    //not sure how much overhead this saves, but whatever
    private static final ThreadLocal<Output> threadOut = new ThreadLocal<Output>(){
        @Override
        protected Output initialValue()
        {
            return new Output(new ByteArrayOutputStream());
        }
    };

    public byte[] toByteArray(DataWrapper input) throws Exception {
        Kryo kryo = threadKryo.get();
        Output out = threadOut.get();
        kryo.writeObject(out, input);
        out.flush();
        byte [] ret = out.getBuffer();
        out.clear();
        return ret;
    }

    public DataWrapper fromByteArray(String key, byte[] input) throws Exception {
        Kryo kryo = threadKryo.get();
        Input ink = new Input(input);
        return (DataWrapper) kryo.readObject(ink, DataWrapper.class);
    }
}