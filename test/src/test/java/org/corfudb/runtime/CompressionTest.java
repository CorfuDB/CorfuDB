package org.corfudb.runtime;

import static org.assertj.core.api.Assertions.assertThat;

import org.corfudb.common.compression.Codec;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.view.AbstractViewTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/**
 * Created by annym on 1/2/2020
 */
@RunWith(Parameterized.class)
public class CompressionTest extends AbstractViewTest {

    private Codec.Type codec;
    private final byte[] DEFAULT_PAYLOAD = "payload".getBytes();

    public CompressionTest(Codec.Type codecType) {
        this.codec = codecType;
    }

    // Static method that generates and returns test data (automatically test for all codec's supported)
    @Parameterized.Parameters
    public static Collection<Codec.Type> input() {
        return Stream.of(Codec.Type.values())
                .collect(Collectors.toList());
    }

    @Before
    public void initialize() {
        // Set a single node Corfu
        addSingleServer(SERVERS.PORT_0);
    }

    private CorfuRuntime getRuntimeWithCodec(Codec.Type codecType) {
        // Instantiate Runtime Client with the given encoder/decoder
        CorfuRuntime rt = getDefaultRuntime();
        rt.getParameters().setCodecType(codecType);
        rt.connect();
        return rt;
    }

    private long writeDefaultPayload(CorfuRuntime rt) {
        // Write / append data to Log
        TokenResponse tokenResponse = rt.getSequencerView().next();
        rt.getAddressSpaceView().write(tokenResponse, DEFAULT_PAYLOAD);

        return tokenResponse.getSequence();
    }

    private Object read(CorfuRuntime rt, long address) {
        // First Invalidate Server & Client Caches
        rt.getAddressSpaceView().invalidateServerCaches();
        rt.getAddressSpaceView().invalidateClientCache();

        // Read data
        return rt.getAddressSpaceView().read(address).getPayload();
    }

    private List<Codec.Type> getReadersCodec(Codec.Type codec) {
        return Stream.of(Codec.Type.values())
                .collect(Collectors.toList());
    }

    /**
     * This test evaluates writing and reading to a stream log with different available codecs.
     * For this test, we setup a single node Corfu, and write/read a single entry with the same runtime.
     */
    @Test
    public void testEndToEndCompression() {
        // Write to Log
        CorfuRuntime rt = getRuntimeWithCodec(codec);
        long address = writeDefaultPayload(rt);

        // Validate read data is the same as written data
        assertThat(read(rt, address)).isEqualTo(DEFAULT_PAYLOAD);
    }

    /**
     * In this test, we validate that reading data from a runtime which is configured with a different codec
     * than that one set on the write path, will not fail.
     */
    @Test
    public void testEndToEndCompressionDifferentCodec() {
        // Write to Log
        CorfuRuntime rt = getRuntimeWithCodec(codec);
        long address = writeDefaultPayload(rt);

        // Get all codec's different from the one used for the write
        List<Codec.Type> codecs = getReadersCodec(codec);

        // Read data from runtime's with different codec's to verify data is retrieved correctly.
        for (Codec.Type readerCodec : codecs) {
            CorfuRuntime readerRt = getRuntimeWithCodec(readerCodec);
            assertThat(read(readerRt, address)).isEqualTo(DEFAULT_PAYLOAD);
        }
    }

    /**
     * Test that interleaved entries with different encoder/decoders are written correctly and
     * read from a single runtime.
     */
    @Test
    public void testInterleavedCompressedEntries() {
        List<Long> addresses = new ArrayList<>();

        input().stream().forEach(codecType -> {
            CorfuRuntime rt = getRuntimeWithCodec(codecType);
            addresses.add(writeDefaultPayload(rt));
        });

        assertThat(addresses.size()).isEqualTo(Codec.Type.values().length);

        CorfuRuntime readerRt = getRuntimeWithCodec(codec);
        addresses.forEach(address -> assertThat(read(readerRt, address)).isEqualTo(DEFAULT_PAYLOAD));
    }
}
