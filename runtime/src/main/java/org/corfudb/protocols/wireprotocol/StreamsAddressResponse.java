package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import java.util.Map;
import java.util.UUID;
import lombok.Value;
import org.corfudb.runtime.view.stream.StreamAddressSpace;

/**
 * Represents the response sent by the sequencer when streams address maps are requested
 *
 * @see org.corfudb.protocols.wireprotocol.StreamsAddressRequest
 *     <p>It contains a per stream map with its corresponding address space (composed of the
 *     addresses of this stream and trim mark)
 */
@Value
public class StreamsAddressResponse implements ICorfuPayload<StreamsAddressResponse> {

  private long logTail;

  private final Map<UUID, StreamAddressSpace> addressMap;

  public StreamsAddressResponse(long logTail, Map<UUID, StreamAddressSpace> streamsAddressesMap) {
    this.logTail = logTail;
    this.addressMap = streamsAddressesMap;
  }

  /**
   * Deserialization Constructor from Bytebuf to StreamsAddressResponse.
   *
   * @param buf The buffer to deserialize
   */
  public StreamsAddressResponse(ByteBuf buf) {
    this.logTail = ICorfuPayload.fromBuffer(buf, Long.class);
    this.addressMap = ICorfuPayload.mapFromBuffer(buf, UUID.class, StreamAddressSpace.class);
  }

  @Override
  public void doSerialize(ByteBuf buf) {
    ICorfuPayload.serialize(buf, this.logTail);
    ICorfuPayload.serialize(buf, this.addressMap);
  }
}
