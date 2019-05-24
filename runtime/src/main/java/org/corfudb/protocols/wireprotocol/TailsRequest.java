package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * A tail request can be specific to the following types:
 * - Global log tail.
 * - Tails for specific streams (uniquely identified).
 * - Tails for all available streams.
 *
 * Whenever tails are requested for streams (specific or all),
 * the global log tail is sent along the response.
 */
@Data
@AllArgsConstructor
public class TailsRequest implements ICorfuPayload<TailsRequest>  {

    public static final byte LOG_TAIL = 0;
    public static final byte STREAMS_TAILS = 1;     /* Log tail is also provided when tails are requested for
                                                    specific streams or all streams*/
    public static final byte ALL_STREAMS_TAIL = 2;

    /** The type of the tail request, one of the above. */
    private final byte reqType;

    /** The streams for which tails are requested. */
    private final List<UUID> streams;

    /**
     * Constructor for TailsRequest.
     *
     * @param streams list of streams identifiers for which tails are requested.
     */
    public TailsRequest(List<UUID> streams) {
        reqType = STREAMS_TAILS;
        this.streams = streams;
    }

    public TailsRequest(byte reqType) {
        this.reqType = reqType;
        this.streams = Collections.EMPTY_LIST;
    }

    /**
     * Deserialization Constructor from Bytebuf to TailsRequest.
     *
     * @param buf The buffer to deserialize
     */
    public TailsRequest(ByteBuf buf) {
        reqType = ICorfuPayload.fromBuffer(buf, Byte.class);

        switch (reqType) {
            case STREAMS_TAILS:
                streams = ICorfuPayload.listFromBuffer(buf, UUID.class);
                break;

            default:
                streams = Collections.EMPTY_LIST;
                break;
        }
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, reqType);
        if (reqType == STREAMS_TAILS) {
            ICorfuPayload.serialize(buf, streams);
        }
    }
}
