package org.corfudb.runtime.collections.remotecorfutable;

import com.google.common.collect.ImmutableMap;
import lombok.Getter;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * This enum contains the string values of the SMR methods for the RemoteCorfuTable.
 *
 * Created by nvaishampayan517 on 08/18/21
 */
public enum RemoteCorfuTableSMRMethods {
    //Arg Format: None
    CLEAR("clear"),
    //Arg Format: Any amount of contiguous Key-Value pairs to add
    UPDATE("put"),
    //Arg Format: Any amount of keys to delete
    DELETE("delete");

    @Getter
    private final String SMRName;

    RemoteCorfuTableSMRMethods(String SMRName) {
        this.SMRName = SMRName;
    }

    private static final Map<String, RemoteCorfuTableSMRMethods> typeMap =
            ImmutableMap.copyOf(Arrays.stream(RemoteCorfuTableSMRMethods.values())
                    .collect(Collectors.toMap(RemoteCorfuTableSMRMethods::getSMRName, Function.identity())));

    public static RemoteCorfuTableSMRMethods getMethodFromName(String name) {
        return typeMap.get(name);
    }
}
