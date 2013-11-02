package edu.berkeley.lipstick.util.serializer;

import java.io.*;
import java.util.Vector;

import com.google.protobuf.ByteString;
import edu.berkeley.lipstick.util.*;

public class ProtoDWSerializer implements IDWSerializer {

    public byte[] toByteArray(DataWrapper input) throws Exception {

        Vector<DWProto.KDSEntryMsg> kdsEntryMsgs = new Vector<DWProto.KDSEntryMsg>();

        KeyDependencySet kds = input.getKeyDependencySet();

        for(String key : kds.getKeys()) {
            KeyDependency kd = kds.getDependency(key);

            Vector<DWProto.ClockEntryMsg> clockEntryMsgs = new Vector<DWProto.ClockEntryMsg>();

            for(String entry : kd.getClock().getWriters()) {
                clockEntryMsgs.add(DWProto.ClockEntryMsg.newBuilder()
                        .setWriter(entry)
                        .setValue(kd.getClock().getValue(entry))
                        .build());
            }

            kdsEntryMsgs.add(DWProto.KDSEntryMsg.newBuilder()
                    .setKey(key)
                    .setWc(DWProto.WriteClockMsg.newBuilder()
                            .addAllEntries(clockEntryMsgs)
                            .build())
                    .build());
        }

        DWProto.KeyDependencySetMsg.Builder kwsb = DWProto.KeyDependencySetMsg.newBuilder();
        kwsb.addAllEntries(kdsEntryMsgs);

        DWProto.KeyDependencySetMsg kdsM = kwsb.build();

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = new ObjectOutputStream(bos);
        out.writeObject(input.getValue());
        out.close();

        DWProto.DataWrapperMsg.Builder dwmb = DWProto.DataWrapperMsg.newBuilder();
        dwmb.setValue(ByteString.copyFrom(bos.toByteArray()));
        dwmb.setKds(kdsM);
        dwmb.setTimestamp(input.getTimestamp());

        DWProto.DataWrapperMsg dwM = dwmb.build();

        return dwM.toByteArray();
    }

    public DataWrapper fromByteArray(String key, byte[] input) throws Exception {
        DWProto.DataWrapperMsg dw = DWProto.DataWrapperMsg.parseFrom(input);

        KeyDependencySet kds = new KeyDependencySet();

        for(DWProto.KDSEntryMsg entry : dw.getKds().getEntriesList())
        {
            WriteClock entryClock = new WriteClock();
            for(DWProto.ClockEntryMsg clockEntry : entry.getWc().getEntriesList()) {
                entryClock.setValue(clockEntry.getWriter(), clockEntry.getValue());
            }

            kds.putDependency(entry.getKey(), new KeyDependency(entryClock));
        }

        ByteArrayInputStream bis = new ByteArrayInputStream(dw.getValue().toByteArray());
        ObjectInput in = new ObjectInputStream(bis);
        Object o = in.readObject();
        in.close();

        DataWrapper ret = new DataWrapper(key, o, kds, dw.getTimestamp());

        return ret;
    }
}