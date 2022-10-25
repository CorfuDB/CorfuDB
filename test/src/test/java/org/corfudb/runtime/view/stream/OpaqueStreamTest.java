package org.corfudb.runtime.view.stream;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.corfudb.protocols.service.CorfuProtocolLogReplication.extractOpaqueEntries;
import static org.corfudb.protocols.service.CorfuProtocolLogReplication.generatePayload;

import com.google.common.reflect.TypeToken;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.assertj.core.api.Assertions;
import org.corfudb.CustomSerializer;
import org.corfudb.protocols.logprotocol.OpaqueEntry;
import org.corfudb.protocols.service.CorfuProtocolLogReplication;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.MultiCheckpointWriter;
import org.corfudb.runtime.collections.PersistentCorfuTable;
import org.corfudb.runtime.exceptions.SerializerException;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.runtime.view.SMRObject;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.Serializers;
import org.junit.Test;

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
