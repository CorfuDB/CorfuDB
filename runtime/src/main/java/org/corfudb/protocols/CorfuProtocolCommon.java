package org.corfudb.protocols;

import com.google.protobuf.ByteString;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.proto.Common.LayoutMsg;
import org.corfudb.runtime.proto.Common.StreamAddressSpaceMsg;
import org.corfudb.runtime.proto.Common.TokenMsg;
import org.corfudb.runtime.proto.Common.UuidMsg;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.stream.StreamAddressSpace;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.io.DataInputStream;
import java.io.DataOutputStream;
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

    public static TokenMsg getTokenMsg(Token token) {
        return TokenMsg.newBuilder()
                .setEpoch(token.getEpoch())
                .setSequence(token.getSequence())
                .build();
    }

    public static StreamAddressSpaceMsg getStreamAddressSpaceMsg(StreamAddressSpace addressSpace) {
        StreamAddressSpaceMsg.Builder addressSpaceMsgBuilder = StreamAddressSpaceMsg.newBuilder();
        addressSpaceMsgBuilder.setTrimMark(addressSpace.getTrimMark());

        try(ByteString.Output bso = ByteString.newOutput()) {
            try(DataOutputStream dos = new DataOutputStream(bso)) {
                addressSpace.getAddressMap().serialize(dos);
                addressSpaceMsgBuilder.setAddressMap(bso.toByteString());
            }
        } catch (Exception ex) {
            log.error("getStreamAddressSpaceMsg: error=[{}, {}] " +
                    "while serializing roaring64NavigableMap", ex, ex.getCause());
        }

        return addressSpaceMsgBuilder.build();
    }

    public static StreamAddressSpace getStreamAddressSpace(StreamAddressSpaceMsg msg) {
        Roaring64NavigableMap roaring64NavigableMap = new Roaring64NavigableMap();

        try(DataInputStream dis = new DataInputStream(msg.getAddressMap().newInput())) {
            roaring64NavigableMap.deserialize(dis);
        } catch (Exception ex) {
            log.error("getStreamAddressSpace: error=[{}, {}] " +
                    "while deserializing roaring64NavigableMap", ex, ex.getCause());
        }

        return new StreamAddressSpace(msg.getTrimMark(), roaring64NavigableMap);
    }
}
