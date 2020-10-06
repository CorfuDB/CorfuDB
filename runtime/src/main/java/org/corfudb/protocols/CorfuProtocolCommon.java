package org.corfudb.protocols;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.proto.Common.LayoutMsg;
import org.corfudb.runtime.proto.Common.UuidMsg;
import org.corfudb.runtime.view.Layout;

import java.util.UUID;

@Slf4j
public class CorfuProtocolCommon {
    public static UuidMsg getUuidMsg(UUID uuid) {
        return UuidMsg.newBuilder()
                .setLsb(uuid.getLeastSignificantBits())
                .setMsb(uuid.getMostSignificantBits())
                .build();
    }

    public static UUID getUUID(UuidMsg uuidMsg) {
        return new UUID(uuidMsg.getLsb(), uuidMsg.getMsb());
    }

    public static LayoutMsg getLayoutMsg(Layout layout) {
        return LayoutMsg.newBuilder()
                .setLayoutJson(layout.asJSONString())
                .build();
    }

    public static Layout getLayout(LayoutMsg layoutMsg) {
        return Layout.fromJSONString(layoutMsg.getLayoutJson());
    }
}
