package org.corfudb.integration;

import com.google.common.reflect.TypeToken;
import io.netty.buffer.ByteBuf;
import org.corfudb.protocols.logprotocol.CheckpointEntry;
import org.corfudb.protocols.logprotocol.ISMRConsumable;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.view.stream.IStreamView;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.JavaSerializer;
import org.corfudb.util.serializer.Serializers;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("checkstyle:magicnumber")
public class StreamLayersIT extends AbstractIT {
    @Test
    public void testParallelDeserialization() throws Exception {

        // Start a corfu server
        Process server_1 = new CorfuServerRunner()
                .setHost(DEFAULT_HOST)
                .setPort(DEFAULT_PORT)
                .setSingle(true)
                .runServer();

        CorfuRuntime.CorfuRuntimeParameters params = CorfuRuntime.CorfuRuntimeParameters
                .builder()
                .cacheDisabled(true)
                .bulkReadSize(100)
                .build();

        // Create write runtime and two read runtimes
        CorfuRuntime writeRuntime = CorfuRuntime.fromParameters(params)
                .parseConfigurationString(DEFAULT_ENDPOINT)
                .setTransactionLogging(true)
                .connect();

        CorfuRuntime parallelRuntime = CorfuRuntime.fromParameters(params)
                .parseConfigurationString(DEFAULT_ENDPOINT)
                .setTransactionLogging(true)
                .connect();

        CorfuRuntime sequentialRuntime = CorfuRuntime.fromParameters(params)
                .parseConfigurationString(DEFAULT_ENDPOINT)
                .setTransactionLogging(true)
                .connect();


        ISerializer serializer = new SlowJavaSerializer((byte) 15);
        Serializers.registerSerializer(serializer);

        // open table
        CorfuTable<Integer, String> testTable = writeRuntime.getObjectsView().build()
                .setStreamName("test_table")
                .setSerializer(serializer)
                .setTypeToken(new TypeToken<CorfuTable<Integer, String>>() {
                })
                .open();

        // write 512 txn records
        int numWrites = 256;
        for (int i = 0; i < numWrites; i++) {
            writeRuntime.getObjectsView().TXBegin();
            testTable.put(i, "Value is " + i);
            writeRuntime.getObjectsView().TXEnd();
        }

        System.out.println("write is done");

        long pos = writeRuntime.getSequencerView().next().getSequence();
        System.out.println("tail is " + pos);

        // parallel stream
        IStreamView parallelStreamView = parallelRuntime.getStreamsView().get(CorfuRuntime.getStreamID("test_table"));
        List<Long> list = new CopyOnWriteArrayList<>();
        long start = System.currentTimeMillis();

        parallelStreamView.streamUpTo(pos)
                .parallel()
                .filter(ILogData::isData)
                .filter(m -> m.getPayload(parallelRuntime) instanceof ISMRConsumable
                        || m.hasCheckpointMetadata())
                .map(ld -> dataAndCheckpointMapper(ld, parallelRuntime, parallelStreamView))
                .flatMap(List::stream)
                .forEachOrdered(smrEntry -> {
                    list.add(smrEntry.getGlobalAddress());
                });
        System.out.println("parallel time is " + (System.currentTimeMillis() - start));
        assertThat(list).isSorted();

        // sequential stream
        IStreamView seqStreamView = sequentialRuntime.getStreamsView().get(CorfuRuntime.getStreamID("test_table"));
        List<Long> seqList = new ArrayList<>();
        long start2 = System.currentTimeMillis();
        seqStreamView.streamUpTo(pos)
                .filter(ILogData::isData)
                .filter(m -> m.getPayload(sequentialRuntime) instanceof ISMRConsumable
                        || m.hasCheckpointMetadata())
                .map(ld -> dataAndCheckpointMapper(ld, sequentialRuntime, seqStreamView))
                .flatMap(List::stream)
                .forEachOrdered(smrEntry -> {
                    seqList.add(smrEntry.getGlobalAddress());
                });
        System.out.println("sequential time is " + (System.currentTimeMillis() - start2));
        assertThat(seqList).isSorted();


        writeRuntime.shutdown();
        parallelRuntime.shutdown();
        sequentialRuntime.shutdown();
        shutdownCorfuServer(server_1);
    }

    private List<SMREntry> dataAndCheckpointMapper(ILogData logData, CorfuRuntime runtime, IStreamView streamView) {
        if (logData.hasCheckpointMetadata()) {
            // This is a CHECKPOINT record.  Extract the SMREntries, if any.
            CheckpointEntry cp = (CheckpointEntry) logData.getPayload(runtime);
            if (cp.getSmrEntries() != null
                    && cp.getSmrEntries().getUpdates().size() > 0) {
                cp.getSmrEntries().getUpdates().forEach(e -> {
                    e.setRuntime(runtime);
                    e.setGlobalAddress(logData.getGlobalAddress());
                });
                return cp.getSmrEntries().getUpdates();
            } else {
                return (List<SMREntry>) Collections.EMPTY_LIST;
            }
        } else {
            return ((ISMRConsumable) logData.getPayload(runtime)).getSMRUpdates(streamView.getId());
        }
    }

    // A slow java serializer
    static class SlowJavaSerializer extends JavaSerializer {

        public SlowJavaSerializer(byte type) {
            super(type);
        }

        @Override
        public Object deserialize(ByteBuf b, CorfuRuntime rt) {
            try {
                TimeUnit.MILLISECONDS.sleep(30);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }

            return super.deserialize(b, rt);
        }
    }
}
