package org.corfudb.protocols.wireprotocol.remotecorfutable;

import com.google.protobuf.ByteString;
import lombok.Data;

/**
 * Returns the value associated to the queried key in the table.
 *
 * Created by nvaishampayan517 08/12/21
 */
@Data
public class GetResponse implements Response {
    private final ByteString value;

    @Override
    public RemoteCorfuTableResponseType getType() {
        return RemoteCorfuTableResponseType.TABLE_ENTRY;
    }
}
