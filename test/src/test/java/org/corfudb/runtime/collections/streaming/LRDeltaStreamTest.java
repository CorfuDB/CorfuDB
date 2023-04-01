package org.corfudb.runtime.collections.streaming;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Address;
import org.junit.Test;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * This class runs all tests from its superclass - DeltaStreamTest, with the stream type as LRDeltaStream.
 */
@SuppressWarnings("checkstyle:magicnumber")
public class LRDeltaStreamTest extends DeltaStreamTest {

    private final List<UUID> streamsTracked = Arrays.asList(UUID.randomUUID(), UUID.randomUUID());

    @Override
    protected void createDeltaStream(long lastAddressRead, int bufferSize) {
        deltaStream = new LRDeltaStream(addressSpaceView, streamId, lastAddressRead, bufferSize, streamsTracked);
    }

    @Override
    protected void verifyExceptionAndErrorBasedOnStreamType() {
        assertThatThrownBy(deltaStream::next)
            .isInstanceOf(IllegalStateException.class)
            .hasMessage(String.format("[%s] does not contain a backpointer to any stream being tracked %s",
                CorfuRuntime.getCheckpointStreamIdFromId(streamId), Arrays.toString(streamsTracked.toArray())));
    }

    @Override
    protected Map<UUID, Long> getTestBackpointerMap() {
        return Collections.singletonMap(streamsTracked.iterator().next(), Address.NON_EXIST);
    }

    @Override
    @Test
    public void badArguments() {
        assertThatThrownBy(() -> new LRDeltaStream(addressSpaceView, streamId,
            -2, 0, streamsTracked))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("lastAddressRead -2 must be >= -1");
        assertThatThrownBy(() -> new LRDeltaStream(addressSpaceView, streamId,
            0, 0, streamsTracked))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("The size must be greater than 0");
    }
}
