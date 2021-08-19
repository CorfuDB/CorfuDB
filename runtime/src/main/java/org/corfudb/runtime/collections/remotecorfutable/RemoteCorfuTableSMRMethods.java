package org.corfudb.runtime.collections.remotecorfutable;

import lombok.Getter;

/**
 * This enum contains the string values of the SMR methods for the RemoteCorfuTable.
 *
 * Created by nvaishampayan517 on 08/18/21
 */
public enum RemoteCorfuTableSMRMethods {
    //Arg Format: Single Argument - timestsamp
    CLEAR("clear"),
    //Arg Format: Long timestamp followed by one or more contiguous Key-Value pairs to add
    UPDATE("put"),
    //Arg Format: Long timestamp followed by one or more of keys to delete
    DELETE("delete");

    @Getter
    private final String SMRName;

    private RemoteCorfuTableSMRMethods(String SMRName) {
        this.SMRName = SMRName;
    }
}
