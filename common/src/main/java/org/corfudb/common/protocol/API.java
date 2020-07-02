package org.corfudb.common.protocol;


import org.corfudb.common.protocol.proto.CorfuProtocol;
import org.corfudb.common.protocol.proto.CorfuProtocol.Header;
import org.corfudb.common.protocol.proto.CorfuProtocol.MessageType;
import org.corfudb.common.protocol.proto.CorfuProtocol.Request;
import org.corfudb.common.protocol.proto.CorfuProtocol.PingRequest;
import org.corfudb.common.protocol.proto.CorfuProtocol.Priority;
import org.corfudb.common.protocol.proto.CorfuProtocol.ProtocolVersion;

import java.util.UUID;

/**
 * Created by Maithem on 7/1/20.
 */

public class API {

    private static CorfuProtocol.UUID getUUID(UUID uuid) {
        return CorfuProtocol.UUID.newBuilder()
                .setLsb(uuid.getLeastSignificantBits())
                .setMsb(uuid.getMostSignificantBits())
                .build();
    }

    public static Header newHeader(ProtocolVersion version, long requestId,
                                   Priority priority, MessageType type,
                                   long epoch, UUID clusterId) {
        return Header.newBuilder()
                .setVersion(version)
                .setRequestId(requestId)
                .setPriority(priority)
                .setType(type)
                .setEpoch(epoch)
                .setClusterId(getUUID(clusterId))
                .build();
    }

    public static Request newPingRequest(Header header) {
        PingRequest pingRequest = PingRequest.getDefaultInstance();
        return Request.newBuilder()
                .setPingRequest(pingRequest)
                .build();
    }

}
