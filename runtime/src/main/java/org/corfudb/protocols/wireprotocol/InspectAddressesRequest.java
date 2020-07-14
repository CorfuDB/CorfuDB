package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * A request message to inspect a list of addresses, which checks if any address in not committed.
 *
 * <p>Created by WenbinZhu on 5/4/20.
 */
@Getter
@AllArgsConstructor
public class InspectAddressesRequest implements ICorfuPayload<InspectAddressesRequest> {

  // List of requested addresses to inspect.
  private final List<Long> addresses;

  /**
   * Deserialization Constructor from ByteBuf to InspectAddressesRequest.
   *
   * @param buf The buffer to deserialize
   */
  public InspectAddressesRequest(ByteBuf buf) {
    addresses = ICorfuPayload.listFromBuffer(buf, Long.class);
  }

  @Override
  public void doSerialize(ByteBuf buf) {
    ICorfuPayload.serialize(buf, addresses);
  }
}
