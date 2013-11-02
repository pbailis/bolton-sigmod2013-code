package edu.berkeley.lipstick.testing;

import edu.berkeley.lipstick.config.Config;
import edu.berkeley.lipstick.util.*;
import edu.berkeley.lipstick.storage.CassandraStorage;
import org.junit.Test;
import org.junit.Ignore;

import java.security.Key;

import static org.junit.Assert.assertEquals;


public class CassandraStorageTest {

    /*
    @Test @Ignore("only run when cassandra is running and configured")
    public void testGetPut() throws Exception {
        CassandraStorage storage = new CassandraStorage();
        storage.open();

        IDWSerializer dws = Config.getDWSerializer();

        storage.put("foo", dws.toByteArray(new DataWrapper("bar1", new KeyDependencySet(), new WriteClock())), System.currentTimeMillis());
        storage.put("foo", dws.toByteArray(new DataWrapper("bar32", new KeyDependencySet(), new WriteClock())), System.currentTimeMillis() - 10000);
        assertEquals("bar1", dws.fromByteArray(storage.get("foo")).getValue());


        storage.close();
    }
    */
}