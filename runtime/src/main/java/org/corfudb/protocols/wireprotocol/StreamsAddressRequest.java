package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import java.util.Collections;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NonNull;

/**
 * Represents the request sent to the sequencer to retrieve one or several streams address map.
 *
 * <p>Created by annym on 02/06/2019
 */
@Data
@AllArgsConstructor
@CorfuPayload
public class StreamsAddressRequest implements ICorfuPayload<StreamsAddressRequest> {

  public static final byte STREAMS = 0; /*To request specific streams*/

  public static final byte ALL_STREAMS = 1; /*To request all streams*/

  /** The type of request, one of the above. */
  final byte reqType;

  private final List<StreamAddressRange> streamsRanges;

  public StreamsAddressRequest(@NonNull List<StreamAddressRange> streamsRanges) {
    reqType = STREAMS;
    this.streamsRanges = streamsRanges;
  }

  public StreamsAddressRequest(Byte reqType) {
    this.reqType = reqType;
    this.streamsRanges = Collections.EMPTY_LIST;
  }

  /**
   * Deserialization Constructor from Bytebuf to StreamsAddressRequest.
   *
   * @param buf The buffer to deserialize
   */
  public StreamsAddressRequest(ByteBuf buf) {
    reqType = ICorfuPayload.fromBuffer(buf, Byte.class);
    if (reqType != ALL_STREAMS) {
      streamsRanges = ICorfuPayload.listFromBuffer(buf, StreamAddressRange.class);
    } else {
      streamsRanges = Collections.EMPTY_LIST;
    }
  }

  @Override
  public void doSerialize(ByteBuf buf) {
    ICorfuPayload.serialize(buf, reqType);
    if (reqType != ALL_STREAMS) {
      ICorfuPayload.serialize(buf, streamsRanges);
    }
  }
}
