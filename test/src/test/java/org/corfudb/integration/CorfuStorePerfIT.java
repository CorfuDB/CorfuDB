package org.corfudb.integration;


import java.lang.reflect.InvocationTargetException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.google.common.reflect.TypeToken;
import org.apache.commons.io.FileUtils;

import org.junit.jupiter.api.Test;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxBuilder;
import org.corfudb.test.SampleSchema;
import org.corfudb.test.SampleSchema.ManagedResources;
import org.corfudb.test.SampleSchema.Uuid;
import org.corfudb.test.SampleSchema.EventInfo;

/**
 * Simple performance test to insert data into corfu via regular table.put() and CorfuStore protobufs
 */
public class CorfuStorePerfIT extends  AbstractIT {

    @Test
    public void corfuStorePerfComparisonTest() throws Exception {
        long maxLogSize = FileUtils.ONE_MB / 2;
        Process server1 = runServerWithQuota(DEFAULT_PORT, maxLogSize,
            true);

        CorfuRuntime rt = new CorfuRuntime(DEFAULT_ENDPOINT).connect();
        final int count = 100;
        addObjectsToTable(rt, count);
        addProtoToStore(rt, count);
    }

    private final static int randomPort = 8000;
    private final static int eventTime = 20000;
    private final static int randomFreq = 10;

    private void addObjectsToTable(CorfuRuntime rt, final int count) {
        System.out.println("Start Writing Java Obj" + System.currentTimeMillis());
        long start = System.currentTimeMillis();
        Map<UUID, Event> map = rt.getObjectsView()
            .build()
            .setStreamName("s1")
            .setTypeToken(new TypeToken<CorfuTable<UUID, Event>>() {})
            .open();

        Event event;
        for(int i=0; i<count; i++) {
            event = new Event(i, "TestEvent"+i, randomPort,
                eventTime, randomFreq
            );
            rt.getObjectsView().TXBegin();
            map.put(UUID.randomUUID(), event);
            rt.getObjectsView().TXEnd();
        }
        long end = System.currentTimeMillis();
        System.out.println("Time Taken: "+ (end - start));
        System.out.println("End Writing Java Obj" + System.currentTimeMillis());
    }

    private void addProtoToStore(CorfuRuntime rt, final int count) {
        // Creating Corfu Store using a connected corfu client.
        CorfuStore corfuStore = new CorfuStore(rt);

        // Define a namespace for the table.
        final String nsxManager = "nsx-manager-namespace5";
        // Define table name.
        final String tableName = "EventInfo";

        System.out.println("Start Writing Proto" + System.currentTimeMillis());
        long start = System.currentTimeMillis();
        // Create & Register the table.
        // This is required to initialize the table for the current corfu client.
        try {
            Table<Uuid, EventInfo, ManagedResources> table = corfuStore.openTable(
                nsxManager,
                tableName,
                Uuid.class,
                EventInfo.class,
                ManagedResources.class,
                // TableOptions includes option to choose - Memory/Disk based corfu table.
                TableOptions.builder().build());
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        List<Uuid> uuids = new ArrayList<>();
        List<EventInfo> events = new ArrayList<>();

        // Fetch timestamp to perform snapshot queries or transactions at a particular timestamp.
        CorfuStoreMetadata.Timestamp timestamp = corfuStore.getTimestamp();

        ManagedResources metadata = ManagedResources.newBuilder().setCreateUser("Pan").build();

        // Creating a transaction builder.
        TxBuilder tx = corfuStore.tx(nsxManager);
        tx.create(tableName, Uuid.newBuilder().setLsb(0L).setMsb(0L).build(),
            SampleSchema.EventInfo.newBuilder().setName("simpleCRUD").build(),
            metadata);
        for (int i = 0; i < count; i++) {
            UUID uuid = UUID.nameUUIDFromBytes(Integer.toString(i).getBytes());
            Uuid uuidMsg = Uuid.newBuilder()
                .setMsb(uuid.getMostSignificantBits())
                .setLsb(uuid.getLeastSignificantBits())
                .build();
            uuids.add(uuidMsg);

            events.add(SampleSchema.EventInfo.newBuilder()
                .setId(i)
                .setName("event_" + i)
                .setEventTime(i)
                .build());

            tx.update(tableName, uuids.get(i), events.get(i), metadata);
            tx.commit();
        }

        long end = System.currentTimeMillis();
        System.out.println("Time Taken: " + (end - start));
        System.out.println("End Writing Proto" + System.currentTimeMillis());
    }

    private Process runServerWithQuota(int port, long quota, boolean singleNode)
        throws Exception {
        String logPath = getCorfuServerLogPath(DEFAULT_HOST, port);
        FileStore corfuDirBackend = Files.getFileStore(Paths.get(CORFU_LOG_PATH));
        long fsSize = corfuDirBackend.getTotalSpace();
        final double HUNDRED = 100.0;
        final double quotaInPerc = quota * HUNDRED / fsSize;
        return new CorfuServerRunner()
            .setHost(DEFAULT_HOST)
            .setPort(port)
            .setSingle(singleNode)
            .setLogPath(logPath)
            .setLogSizeLimitPercentage(Double.toString(quotaInPerc))
            .runServer();
        }
}

