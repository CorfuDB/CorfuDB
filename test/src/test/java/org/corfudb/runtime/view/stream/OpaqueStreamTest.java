package org.corfudb.runtime.view.stream;

import com.google.common.reflect.TypeToken;
import org.corfudb.CustomSerializer;
import org.corfudb.protocols.logprotocol.MultiSMREntry;
import org.corfudb.protocols.logprotocol.OpaqueEntry;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.MultiCheckpointWriter;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.exceptions.SerializerException;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.Serializers;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import org.junit.Test;

import java.util.UUID;
import java.util.stream.Stream;

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
        OpaqueStream opaqueStream = new OpaqueStream(rt2, sv);
        final long snapshot = 100;
        Stream<OpaqueEntry> stream = opaqueStream.streamUpTo(snapshot);

        stream.forEach(entry -> {
            System.out.println(entry.getVersion() + " " + entry.getEntries());
        });

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
        OpaqueStream opaqueStream = new OpaqueStream(rt2, sv);
        final long snapshot = 100;
        Stream<OpaqueEntry> stream = opaqueStream.streamUpTo(snapshot);

        stream.forEach(entry -> {
            System.out.println(entry.getVersion() + " " + entry.getEntries());
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
