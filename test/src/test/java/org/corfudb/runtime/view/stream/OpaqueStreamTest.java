package org.corfudb.runtime.view.stream;

import com.google.common.reflect.TypeToken;
import lombok.extern.slf4j.Slf4j;
import org.assertj.core.api.Assertions;
import org.corfudb.CustomSerializer;
import org.corfudb.infrastructure.logreplication.proto.Sample;
import org.corfudb.protocols.logprotocol.OpaqueEntry;
import org.corfudb.protocols.service.CorfuProtocolLogReplication;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.MultiCheckpointWriter;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.PersistentCorfuTable;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.exceptions.SerializerException;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.Serializers;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.corfudb.integration.LogReplicationAbstractIT.NAMESPACE;
import static org.corfudb.protocols.service.CorfuProtocolLogReplication.extractOpaqueEntries;
import static org.corfudb.protocols.service.CorfuProtocolLogReplication.generatePayload;

@Slf4j
public class OpaqueStreamTest extends AbstractViewTest {

    private final int MVO_CACHE_SIZE = 100;

    @Test
    public void testMagicByte() {
        CorfuRuntime rt = getDefaultRuntime();

        ISerializer customSerializer = new CustomSerializer((byte) (Serializers.SYSTEM_SERIALIZERS_COUNT + 2));
        rt.getSerializers().registerSerializer(customSerializer);
        rt.getParameters().setMaxMvoCacheEntries(MVO_CACHE_SIZE);

        UUID streamId = CorfuRuntime.getStreamID("stream1");

        PersistentCorfuTable<Integer, Integer> table = rt.getObjectsView()
                .build()
                .setStreamID(streamId)
                .setTypeToken(new TypeToken<PersistentCorfuTable<Integer, Integer>>() {})
                .setSerializer(customSerializer)
                .open() ;


        rt.getObjectsView().TXBegin();
        final int key1 = 1;
        final int key2 = 2;
        final int key3 = 3;
        table.insert(key1, key1);
        table.insert(key2, key2);
        table.insert(key3, key3);
        rt.getObjectsView().TXEnd();


        rt.getSerializers().removeSerializer(customSerializer);

        CorfuRuntime rt2 = getNewRuntime(getDefaultNode()).connect();
        IStreamView sv = rt2.getStreamsView().get(streamId);
        OpaqueStream opaqueStream = new OpaqueStream(sv);
        final long snapshot = 100;
        Stream<OpaqueEntry> stream = opaqueStream.streamUpTo(snapshot);

        stream.forEach(entry -> {
            log.debug(entry.getVersion() + " " + entry.getEntries());
        });

    }

    /**
     * Ensure that {@link CorfuProtocolLogReplication#extractOpaqueEntries} and
     * {@link CorfuProtocolLogReplication#generatePayload} are inverse of one another.
     */
    @Test
    public void extractAndGenerate() {
        CorfuRuntime runtime = getDefaultRuntime();
        ISerializer customSerializer = new CustomSerializer((byte) (Serializers.SYSTEM_SERIALIZERS_COUNT + 2));
        runtime.getSerializers().registerSerializer(customSerializer);
        runtime.getParameters().setMaxMvoCacheEntries(MVO_CACHE_SIZE);

        UUID streamId = UUID.randomUUID();

        PersistentCorfuTable<Integer, Integer> table = runtime.getObjectsView()
                .build()
                .setStreamID(streamId)
                .setTypeToken(new TypeToken<PersistentCorfuTable<Integer, Integer>>() {})
                .setSerializer(customSerializer)
                .open() ;

        final int entryCount = 3;
        runtime.getObjectsView().TXBegin();
        for (int key = 0; key < entryCount; key++) {
            table.insert(key, key);
        }
        runtime.getObjectsView().TXEnd();

        runtime.getSerializers().removeSerializer(customSerializer);

        CorfuRuntime newRuntime = getNewRuntime(getDefaultNode()).connect();

        IStreamView streamView = newRuntime.getStreamsView().get(streamId);
        OpaqueStream opaqueStream = new OpaqueStream(streamView);
        List<OpaqueEntry> entries = opaqueStream.streamUpTo(Integer.MAX_VALUE).collect(Collectors.toList());
        Assertions.assertThat(extractOpaqueEntries(generatePayload(entries)).size()).isEqualTo(entries.size());
    }


    /**
     * 1. Create a runtime. By default "RetainSerializedDataInCache" is false.
     * 2. Write 3 records, and deserialize them by calling LogData.getPayload().
     * 3.Ensure that the data part is null and observe the opaqueStream skipping those entries.
     * ------
     * 4. Now set "RetainSerializedDataInCache" as true.
     * 5. Write 3 more records, and deserialize them by calling LogData.getPayload()
     * 6. The data part will not be not-null, and opaque stream will be able to read those entries.
     */
    @Test
    public void deserializeDataAndGenerateOpaqueStream() throws Exception {
        CorfuRuntime runtime = getDefaultRuntime();
        CorfuStore store = new CorfuStore(runtime);
        Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> tableSource =
                store.openTable(NAMESPACE, "test", Sample.StringKey.class,
                        Sample.IntValueTag.class, Sample.Metadata.class,
                        TableOptions.fromProtoSchema(Sample.IntValueTag.class));

        final int entryCount = 3;
        List<Long> addressWrittenTo = new ArrayList<>();
        for (int i = 0; i < entryCount; i++) {
            Sample.StringKey stringKey = Sample.StringKey.newBuilder().setKey(String.valueOf(i)).build();
            Sample.IntValueTag intValueTag = Sample.IntValueTag.newBuilder().setValue(i).build();
            Sample.Metadata metadata = Sample.Metadata.newBuilder().setMetadata("Metadata_" + i).build();
            try (TxnContext txn = store.txn(NAMESPACE)) {
                txn.putRecord(tableSource, stringKey, intValueTag, metadata);
                addressWrittenTo.add(txn.commit().getSequence());
            }
        }

        validateOpaqueStream(runtime, 0, addressWrittenTo);

        runtime.getParameters().setRetainSerializedDataInCache(true);
        validateOpaqueStream(runtime, entryCount, addressWrittenTo);
    }

    private void validateOpaqueStream(CorfuRuntime runtime, int expected, List<Long> addressWrittenTo) {
        //invalidate the cache, so the data is read from the logUnit server
        runtime.getAddressSpaceView().invalidateClientCache();

        // read the previously written data
        for(long address : addressWrittenTo) {
            LogData logData = (LogData) runtime.getAddressSpaceView().read(address);
            assertThat(logData.getData()).isNotEmpty();

            // verify the "data" part after the logData is deserialized
            logData.getPayload(runtime);
            if(runtime.getParameters().isRetainSerializedDataInCache()) {
                assertThat(logData.getData()).isNotEmpty();
            } else {
                assertThat(logData.getData()).isNullOrEmpty();
            }
        }

        IStreamView streamView = runtime.getStreamsView().get(CorfuRuntime.getStreamID(TableRegistry.getFullyQualifiedTableName(NAMESPACE,
                "test")));
        OpaqueStream opaqueStream = new OpaqueStream(streamView);
        List<OpaqueEntry> entries = opaqueStream.streamUpTo(Integer.MAX_VALUE).collect(Collectors.toList());

        assertThat(entries.size()).isEqualTo(expected);
    }

    @Test
    public void testBasicStreaming() {
        CorfuRuntime rt = getDefaultRuntime();

        ISerializer customSerializer = new CustomSerializer((byte) (Serializers.SYSTEM_SERIALIZERS_COUNT + 2));
        rt.getSerializers().registerSerializer(customSerializer);
        rt.getParameters().setMaxMvoCacheEntries(MVO_CACHE_SIZE);

        UUID streamId = CorfuRuntime.getStreamID("stream1");

        PersistentCorfuTable<Integer, Integer> table = rt.getObjectsView()
                .build()
                .setStreamID(streamId)
                .setTypeToken(new TypeToken<PersistentCorfuTable<Integer, Integer>>() {})
                .setSerializer(customSerializer)
                .open();


        final int key1 = 1;
        final int key2 = 2;
        final int key3 = 3;
        table.insert(key1, key1);
        table.insert(key2, key2);
        table.insert(key3, key3);

        MultiCheckpointWriter<PersistentCorfuTable<Integer, Integer>> mcw = new MultiCheckpointWriter<>();
        mcw.addMap(table);
        Token token = mcw.appendCheckpoints(rt, "baah");

        rt.getAddressSpaceView().prefixTrim(token);

        rt.getSerializers().removeSerializer(customSerializer);

        CorfuRuntime rt2 = getNewRuntime(getDefaultNode()).connect();
        IStreamView sv = rt2.getStreamsView().get(streamId);
        OpaqueStream opaqueStream = new OpaqueStream(sv);
        final long snapshot = 100;
        Stream<OpaqueEntry> stream = opaqueStream.streamUpTo(snapshot);

        stream.forEach(entry -> {
            log.debug(entry.getVersion() + " " + entry.getEntries());
        });

        CorfuRuntime rt3 = getNewRuntime(getDefaultNode()).connect();

        PersistentCorfuTable<Integer, Integer> table2 = rt3.getObjectsView()
                .build()
                .setStreamID(streamId)
                .setTypeToken(new TypeToken<PersistentCorfuTable<Integer, Integer>>() {})
                .open() ;

        assertThatThrownBy(() -> table2.size()).isInstanceOf(SerializerException.class);
    }
}
