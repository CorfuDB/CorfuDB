package org.corfudb.protocols.wireprotocol.remotecorfutable;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Types of RemoteCorfuTable responses
 *
 * Created by nvaishampayan517 on 08/12/21
 */
@AllArgsConstructor
public enum RemoteCorfuTableResponseType {
    /**
     * Single Entry Response - from GET
     */
    TABLE_ENTRY(0),
    /**
     * Multi Entry Response - from SCAN
     */
    TABLE_ENTRY_LIST(1),
    /**
     * Size response - from SIZE
     */
    TABLE_SIZE(2),
    /**
     * Item contained response - from CONTAINSKEY, CONTAINSVALUE
     */
    TABLE_ITEM_CONTAINED(3);

    @Getter
    public final int type;
}
