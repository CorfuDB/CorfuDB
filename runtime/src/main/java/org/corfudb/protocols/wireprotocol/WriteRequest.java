package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import java.util.EnumMap;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

/** Created by mwei on 8/9/16. */
@Builder
@AllArgsConstructor
public class WriteRequest implements ICorfuPayload<WriteRequest>, IMetadata {

  @Getter final ILogData data;

  @SuppressWarnings("unchecked")
  public WriteRequest(ByteBuf buf) {
    this.data = ICorfuPayload.fromBuffer(buf, LogData.class);
  }

  public WriteRequest(DataType dataType, ByteBuf buf) {
    this.data = new LogData(dataType, buf);
  }

  @Override
  public void doSerialize(ByteBuf buf) {
    ICorfuPayload.serialize(buf, data);
  }

  @Override
  public EnumMap<LogUnitMetadataType, Object> getMetadataMap() {
    return data.getMetadataMap();
  }
}
