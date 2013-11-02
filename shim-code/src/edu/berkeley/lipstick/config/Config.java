package edu.berkeley.lipstick.config;
import edu.berkeley.lipstick.backend.IExplicitBackend;
import edu.berkeley.lipstick.localstore.ILocalStore;
import edu.berkeley.lipstick.storage.IStorage;
import edu.berkeley.lipstick.util.serializer.IDWSerializer;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.util.Map;

public class Config {
    private static IStorage storage;

    private static Object backend;
    private static Map<String, Object> configOptions;
    private static ILocalStore localStore;

    private static String lipstickConfigEnvString = "lipstick.config";
    private static String lipstickPidString = "lipstick.pid";
    private static String simpleStorageHostString = "simple.storage.host";
    private static String localStoreClassString = "localstore.class";
    private static String simpleStoragePortString = "simple.storage.port";
    private static String localstoreKyotoFilePathString = "localstore.kyoto.filepath";
    private static String readLocalOnlyString = "backend.read.localonly";

    private static String storageClassString = "storage.class";
    private static String backendClassString = "backend.class";
    private static String dwSerializerClassString = "dw.serializer.class";
    private static String cassandraConsistencyLevelString = "cassandra.consistencylevel";
    private static String cassandraNodeIPString = "cassandra.node.ip";
    private static String cassandraNodePortString = "cassandra.node.port";
    private static String cassandraKeyspaceString = "cassandra.keyspace";
    private static String cassandraColumnFamilyString = "cassandra.columnfamily";
    private static String backendAsyncSleepMSString = "backend.async.sleepms";
    private static String resolveInBackgroundString = "backend.asyncresolve";
    private static String backendMaxECDSString = "backend.maxsyncECDSreads";
    private static String backendMaxKeysToCheck = "backend.maxKeysToCheck";
    private static String backendMaxBufferedWrites = "backend.maxBufferedWrites";

    static void getConfig() throws Exception {
        if(configOptions != null)
            return;

        configOptions = (Map<String, Object>) (new Yaml()).load(new FileInputStream(new File(
                                                               System.getProperty(lipstickConfigEnvString))));
    }

    public static String getProcessID() throws Exception {
        getConfig();

        assert(configOptions.get(lipstickPidString ) != null);

        return (String)configOptions.get(lipstickPidString );
    }

    public static long getBytesRead() throws Exception {
        getConfig();

        return storage.getBytesRead();
    }

    public static long getBytesWritten() throws Exception {
        getConfig();

        return storage.getBytesWritten();
    }

    public static String getSimpleBackendHost() throws Exception {
        getConfig();

        assert(configOptions.get(simpleStorageHostString) != null);

        return (String)configOptions.get(simpleStorageHostString);
    }

    public static ILocalStore getLocalStore() throws Exception {
        getConfig();

        if(localStore == null) {
            Class newLocalStore = Class.forName((String)configOptions.get(localStoreClassString));
            localStore = (ILocalStore) newLocalStore.newInstance();
        }

        return localStore;
    }


     public static int getSimpleBackendPort() throws Exception {
        getConfig();

        assert(configOptions.get(simpleStoragePortString) != null);

        return (Integer)configOptions.get(simpleStoragePortString);
    }

    public static String getKyotoFilePath() throws Exception {
        getConfig();

        assert(configOptions.get(localstoreKyotoFilePathString) != null);

        return (String)configOptions.get(localstoreKyotoFilePathString);
    }

    public static IExplicitBackend getExplicitBackend() throws Exception {
        getConfig();
        if(storage == null) {
            Class storageClass = Class.forName((String)configOptions.get(storageClassString));
            storage = (IStorage) storageClass.newInstance();
        }

        if(backend == null) {
            Class backendClass = Class.forName((String)configOptions.get(backendClassString));
            backend = backendClass.getDeclaredConstructor(IStorage.class).newInstance(storage);
        }

        //we're going to fail in the next line, but this is no longer sane anyway
        assert(backend instanceof IExplicitBackend);

        return (IExplicitBackend) backend;
    }

     public static IDWSerializer getDWSerializer() throws Exception {
        getConfig();
        Class serializerClass = Class.forName((String)configOptions.get(dwSerializerClassString));
        return (IDWSerializer) serializerClass.newInstance();
     }

    public static String getCassandraConsistencyLevel() throws Exception {
        getConfig();

        assert(configOptions.get(cassandraConsistencyLevelString) != null);

        return (String)configOptions.get(cassandraConsistencyLevelString);

    }

     public static String getCassandraIP() throws Exception {
        getConfig();

        assert(configOptions.get(cassandraNodeIPString) != null);

        return (String)configOptions.get(cassandraNodeIPString);
    }

     public static Integer getCassandraPort() throws Exception {
        getConfig();

        assert(configOptions.get(cassandraNodePortString) != null);

        return (Integer)configOptions.get(cassandraNodePortString);
    }

     public static String getCassandraKeyspace() throws Exception {
        getConfig();

        assert(configOptions.get(cassandraKeyspaceString) != null);

        return (String)configOptions.get(cassandraKeyspaceString);
     }

     public static String getCassandraColumnFamily() throws Exception {
        getConfig();

        assert(configOptions.get(cassandraColumnFamilyString) != null);

        return (String)configOptions.get(cassandraColumnFamilyString);
    }

     public static long getAsyncSleepLength() throws Exception {
        getConfig();

        assert(configOptions.get(backendAsyncSleepMSString) != null);
        Long l = new Long((Integer)configOptions.get(backendAsyncSleepMSString));
        return l.longValue();
    }

    public static long getStorageWriteLatency() throws Exception {
        if(storage == null)
            return 0;

        return storage.getWriteLatency();
    }

    public static long getStorageReadLatency() throws Exception {
        if(storage == null)
            return 0;

        return storage.getReadLatency();
    }

    public static boolean readLocalOnly() throws Exception {
        getConfig();

        assert(configOptions.get(readLocalOnlyString) != null);

        return (Boolean) configOptions.get(readLocalOnlyString);
    }

    public static boolean doResolveInBackground() throws Exception {
        getConfig();

        assert(configOptions.get(resolveInBackgroundString) != null);

        return (Boolean) configOptions.get(resolveInBackgroundString);
    }

    public static int getBackendMaxSyncECDSReads() throws Exception {
        getConfig();

        assert(configOptions.get(backendMaxECDSString) != null);

        if(configOptions.get(backendMaxECDSString).equals("infinity"))
            return Integer.MAX_VALUE;

        return (Integer) configOptions.get(backendMaxECDSString);
    }

    public static int getMaxKeysToCheck() throws Exception {
        getConfig();

        assert(configOptions.get(backendMaxKeysToCheck) != null);

        return (Integer) configOptions.get(backendMaxKeysToCheck);
    }

    public static int getMaxBufferedWrites() throws Exception {
        getConfig();

        assert(configOptions.get(backendMaxBufferedWrites) != null);

        return (Integer) configOptions.get(backendMaxBufferedWrites);
    }
}
