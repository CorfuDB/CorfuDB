package org.corfudb.protocols;

import com.google.protobuf.ByteString;
import static org.corfudb.protocols.CorfuProtocolCommon.getUuidMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestPayloadMsg;
import org.corfudb.runtime.proto.service.RemoteCorfuTable.RemoteCorfuTableGetRequestMsg;
import org.corfudb.runtime.proto.service.RemoteCorfuTable.RemoteCorfuTableRequestMsg;

import java.util.UUID;

/**
 * This class provides methods for creating the Protobuf objects defined
 * in remote_corfu_table.proto
 *
 * <p>Created by nvaishampayan517 on 8/5/21.
 */
public class CorfuProtocolRemoteCorfuTable {
    // Prevent class from being instantiated
    private CorfuProtocolRemoteCorfuTable() {}

    /**
     * Returns a REMOTE CORFU TABLE request message containing a RemoteCorfuTableGetRequestMsg
     * that can be sent by the client. Used to request values from the server side database backing
     * the RemoteCorfuTable.
     * @param streamID The UUID of the stream backing the RemoteCorfuTable.
     * @param versionedKey The versioned key
     * @return
     */
    public static RequestPayloadMsg getGetRequestMsg(UUID streamID, ByteString versionedKey) {
        return RequestPayloadMsg.newBuilder()
                .setRemoteCorfuTableRequest(RemoteCorfuTableRequestMsg.newBuilder()
                    .setGet(RemoteCorfuTableGetRequestMsg.newBuilder()
                        .setStreamID(getUuidMsg(streamID))
                        .setVersionedKey(versionedKey)
                        .build())
                    .build())
                .build();
    }
}
