package org.corfudb;

import com.beust.jcommander.Parameter;
import com.google.protobuf.Message;
import lombok.Getter;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.ExampleSchemas.ExampleKey;
import org.corfudb.runtime.ExampleSchemas.ExampleValue;
import org.corfudb.runtime.ExampleSchemas.ManagedMetadata;
import org.corfudb.runtime.Queue;
import org.corfudb.runtime.collections.*;

import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

@Getter
public class CommonUtils {
    public static class Config {
        @Parameter(names = "--configFile", description = "To subscribe to the tables") //TODO
        String configFile;
        @Parameter(names = "--listWorkflows", description = "List of workflows to be performed")
        List<String> listWorkflows = new ArrayList<>();
        @Parameter(names = "--listWorkflowPropFiles", description = "Number of reader threads") //TODO
        List<String> listWorkflowPropFiles = new ArrayList<>();

        @Parameter(names = "--testDuration", description = "Num of minutes to run the test")
        int testDuration;

        @Parameter(names = "--host", description = "Host to connect to")
        String host;

        @Parameter(names = "--port", description = "Port to connect to")
        String port;
    }

    public static final String EXAMPLE_VALUE_PREFIX = "Value_";
    public static final String EXAMPLE_KEY_PREFIX = "Key_";
    public static final int DEFAULT_BOUND = 100;
    CorfuRuntime corfuRuntime;
    CorfuStore corfuStore;
    Map<String, Table<ExampleKey, ? extends Message, ManagedMetadata>> openedTables = new HashMap<>();
    Map<String, Table<Queue.CorfuGuidMsg, ExampleValue, Queue.CorfuQueueMetadataMsg>> openedQueues = new HashMap<>();

    CommonUtils(CorfuStore corfuStore) {
        corfuRuntime = corfuStore.getRuntime();
        this.corfuStore = corfuStore;
    }

    public void openTable(String namespace, String tableName)
            throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        openTable(namespace, tableName, ExampleValue.class);
    }

    public <V extends Message> void openTable(String namespace,
                                              String tableName,
                                              Class<V> vClass)
            throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        openedTables.put(tableName, corfuStore.openTable(namespace,
                tableName,
                ExampleKey.class,
                vClass,
                ManagedMetadata.class,
                TableOptions.fromProtoSchema(ExampleValue.class)));
    }

    public void openQueue(String namespace, String queueName) throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        openedQueues.put(queueName, corfuStore.openQueue(namespace,
                queueName,
                ExampleValue.class,
                TableOptions.fromProtoSchema(ExampleValue.class)));
    }

    public Table<Queue.CorfuGuidMsg, ExampleValue, Queue.CorfuQueueMetadataMsg> getQueue(String namespace, String tableName) {
        return (Table<Queue.CorfuGuidMsg, ExampleValue, Queue.CorfuQueueMetadataMsg>) openedQueues.get(tableName);
    }

    public <V extends Message> Table<ExampleKey, V, ManagedMetadata> getTable(String namespace, String tableName) {
        return (Table<ExampleKey, V, ManagedMetadata>) openedTables.get(tableName);
    }

    public ExampleKey getRandomKey(long bound) {
        return ExampleKey.newBuilder().setKey(EXAMPLE_KEY_PREFIX + ThreadLocalRandom.current().nextLong(bound)).build();
    }

    public ExampleValue getRandomValue(int payloadSize) {
        return getRandomValue(100, payloadSize);
    }

    public ExampleValue getRandomValue(long bound, int payloadSize) {
        byte[] payload = new byte[payloadSize];
        ThreadLocalRandom.current().nextBytes(payload);
        return ExampleValue.newBuilder()
                .setPayload(Arrays.toString(payload))
                .setAnotherKey(ThreadLocalRandom.current().nextLong(bound))
                .build();
    }
}


