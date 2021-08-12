package org.corfudb.protocols.wireprotocol.remotecorfutable;

import lombok.Data;

/**
 * Returns integer representing size of queried table.
 *
 * Created by nvaishampayan517 on 08/12/21.
 */
@Data
public class SizeResponse implements Response {
    private final int size;

    @Override
    public RemoteCorfuTableResponseType getType() {
        return RemoteCorfuTableResponseType.TABLE_SIZE;
    }
}
