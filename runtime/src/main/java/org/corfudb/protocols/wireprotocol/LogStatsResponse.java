package org.corfudb.protocols.wireprotocol;

import lombok.Getter;
import lombok.Setter;
import lombok.Value;
import io.netty.buffer.ByteBuf;

/**
 * Represents the response sent by the log server when query the log stats
 * @see org.corfudb.protocols.wireprotocol.LogStatsResponse
 *
 * It contains the quota used and log size
 */
@Value
public class LogStatsResponse implements ICorfuPayload<LogStatsResponse>{

    @Getter
    private long usedQuota;

    @Getter
    private long limit;

    @Getter
    @Setter
    long logSize;

    public LogStatsResponse(long usedQuota, long limit, long logSize) {
        this.usedQuota = usedQuota;
        this.limit = limit;
        this.logSize = logSize;
    }
    /**
     * Deserialization Constructor from Bytebuf to StreamsAddressResponse.
     *
     * @param buf The buffer to deserialize
     */
    public LogStatsResponse(ByteBuf buf) {
        this.usedQuota = ICorfuPayload.fromBuffer(buf, Long.class);
        this.limit = ICorfuPayload.fromBuffer(buf, Long.class);
        this.logSize = ICorfuPayload.fromBuffer(buf, Long.class);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, this.usedQuota);
        ICorfuPayload.serialize(buf, this.limit);
        ICorfuPayload.serialize(buf, this.logSize);
    }
}
