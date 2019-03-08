package org.corfudb.protocols.wireprotocol;

import lombok.Data;

import java.util.UUID;

/**
 * This class represents a range of addresses for a stream.
 *
 * This is used to request the address map of a stream in
 * a given boundary (limits given by start and end).
 *
 * Created by annym on 03/06/19
 */
@Data
public class StreamAddressRange {

    public final UUID streamID;
    public final long start;
    public final long end;
}
