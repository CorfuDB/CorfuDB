package org.corfudb.protocols.wireprotocol.remotecorfutable;

import lombok.Data;

/**
 * Returns a boolean to indicate if the queried item is contained in the table.
 *
 * Created by nvaishampayan517 on 08/12/21
 */
@Data
public class ContainsResponse implements Response {
    private final boolean contained;

    @Override
    public RemoteCorfuTableResponseType getType() {
        return RemoteCorfuTableResponseType.TABLE_ITEM_CONTAINED;
    }
}
