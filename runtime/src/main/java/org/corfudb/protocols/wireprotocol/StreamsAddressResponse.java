package org.corfudb.protocols.wireprotocol;

import lombok.Data;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.stream.StreamAddressSpace;

import java.util.Map;
import java.util.UUID;

/**
 * Represents the response sent by the sequencer when streams address maps are requested.
 * It contains a per stream map with its corresponding address space, composed of the
 * addresses of this stream and trim mark.
 */
@Data
public class StreamsAddressResponse {

    private long logTail;

    private long epoch = Layout.INVALID_EPOCH;

    private final Map<UUID, StreamAddressSpace> addressMap;

    public StreamsAddressResponse(long logTail, Map<UUID, StreamAddressSpace> streamsAddressesMap) {
        this.logTail = logTail;
        this.addressMap = streamsAddressesMap;
    }
}
