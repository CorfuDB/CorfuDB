package org.corfudb.runtime.collections.streaming;

import lombok.Getter;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.view.AddressSpaceView;
import java.util.Arrays;
import java.util.Set;
import java.util.UUID;

@Slf4j
public class LRDeltaStream extends DeltaStream {

    @Getter
    private final Set<UUID> streamsTracked;

    public LRDeltaStream(AddressSpaceView addressSpaceView, UUID streamId, long lastAddressRead, int bufferSize,
                         Set<UUID> streamsTracked) {
        super(addressSpaceView, streamId, lastAddressRead, bufferSize);
        this.streamsTracked = streamsTracked;
    }

    @Override
    protected void validateBackpointerPresence(ILogData logData) {
        Set<UUID> streamsWithBackpointer = logData.getBackpointerMap().keySet();
        for (UUID stream : streamsTracked) {
            if (streamsWithBackpointer.contains(stream)) {
                return;
            }
        }
        throw new IllegalStateException(String.format("%s does not contain a backpointer to any stream being tracked %s",
                streamsWithBackpointer, Arrays.toString(streamsTracked.toArray())));
    }
}
