package org.corfudb.protocols.wireprotocol;

import lombok.Value;

import java.util.UUID;

/**
 * This class represents a range of addresses for a stream.
 * <p>
 * This is used to request the address map of a stream in
 * a given boundary-- limits given by (end, start].
 * <p>
 * Created by annym on 03/06/19
 */
@Value
public class StreamAddressRange {

    private final UUID streamID;
    // Start is inclusive
    private final long start;

    // Stop is exclusive
    private final long end;
}
