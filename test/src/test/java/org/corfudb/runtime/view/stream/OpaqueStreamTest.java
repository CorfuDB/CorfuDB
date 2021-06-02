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
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.exceptions.SerializerException;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.Serializers;
import org.junit.Test;

@Slf4j
public class OpaqueStreamTest extends AbstractViewTest {

    @Test
    public void testMagicByte() {
        CorfuRuntime rt = getDefaultRuntime();

        ISerializer customSerializer = new CustomSerializer((byte) (Serializers.SYSTEM_SERIALIZERS_COUNT + 2));
        Serializers.registerSerializer(customSerializer);

        UUID streamId = CorfuRuntime.getStreamID("stream1");

        CorfuTable<Integer, Integer> map = rt.getObjectsView()
                .build()
                .setStreamID(streamId)
                .setTypeToken(new TypeToken<CorfuTable<Integer, Integer>>() {})
                .setSerializer(customSerializer)
                .open() ;


        rt.getObjectsView().TXBegin();
        final int key1 = 1;
        final int key2 = 2;
        final int key3 = 3;
        map.put(key1, key1);
        map.put(key2, key2);
        map.put(key3, key3);
        rt.getObjectsView().TXEnd();


        Serializers.removeSerializer(customSerializer);

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
        Serializers.registerSerializer(customSerializer);

        UUID streamId = UUID.randomUUID();

        CorfuTable<Integer, Integer> map = runtime.getObjectsView()
                .build()
                .setStreamID(streamId)
                .setTypeToken(new TypeToken<CorfuTable<Integer, Integer>>() {})
                .setSerializer(customSerializer)
                .open() ;

        final int entryCount = 3;
        runtime.getObjectsView().TXBegin();
        for (int key = 0; key < entryCount; key++) {
            map.put(key, key);
        }
        runtime.getObjectsView().TXEnd();

        Serializers.removeSerializer(customSerializer);

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
        Serializers.registerSerializer(customSerializer);

        UUID streamId = CorfuRuntime.getStreamID("stream1");

        CorfuTable<Integer, Integer> map = rt.getObjectsView()
                .build()
                .setStreamID(streamId)
                .setTypeToken(new TypeToken<CorfuTable<Integer, Integer>>() {})
                .setSerializer(customSerializer)
                .open() ;


        final int key1 = 1;
        final int key2 = 2;
        final int key3 = 3;
        map.put(key1, key1);
        map.put(key2, key2);
        map.put(key3, key3);

        MultiCheckpointWriter mcw = new MultiCheckpointWriter();
        mcw.addMap(map);
        Token token = mcw.appendCheckpoints(rt, "baah");

        rt.getAddressSpaceView().prefixTrim(token);

        Serializers.removeSerializer(customSerializer);

        CorfuRuntime rt2 = getNewRuntime(getDefaultNode()).connect();

        IStreamView sv = rt2.getStreamsView().get(streamId);
        OpaqueStream opaqueStream = new OpaqueStream(sv);
        final long snapshot = 100;
        Stream<OpaqueEntry> stream = opaqueStream.streamUpTo(snapshot);

        stream.forEach(entry -> {
            log.debug(entry.getVersion() + " " + entry.getEntries());
        });

        CorfuRuntime rt3 = getNewRuntime(getDefaultNode()).connect();

        CorfuTable<Integer, Integer> map2 = rt3.getObjectsView()
                .build()
                .setStreamID(streamId)
                .setTypeToken(new TypeToken<CorfuTable<Integer, Integer>>() {})
                .open() ;

        assertThatThrownBy(() -> map2.size()).isInstanceOf(SerializerException.class);
    }
}
