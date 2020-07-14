package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Response for known addresses in the log unit server for a specified range. Created by
 * zlokhandwala on 2019-06-01.
 */
@Data
@AllArgsConstructor
public class KnownAddressResponse implements ICorfuPayload<KnownAddressResponse> {

  private final Set<Long> knownAddresses;

  /**
   * Deserialization Constructor from Bytebuf to KnownAddressRequest.
   *
   * @param buf The buffer to deserialize
   */
  public KnownAddressResponse(ByteBuf buf) {
    knownAddresses = ICorfuPayload.setFromBuffer(buf, Long.class);
  }

  @Override
  public void doSerialize(ByteBuf buf) {
    ICorfuPayload.serialize(buf, knownAddresses);
  }
}
