package org.corfudb.runtime.exceptions;

import lombok.Getter;

@Getter
public enum OverwriteCause {
    /** Indicates this address has been already written by a hole.*/
    HOLE(0),
    /** Indicates this address has been already written and it is the same data..*/
    SAME_DATA(1),
    /** Indicates this address has been already written and it is a different data.*/
    DIFF_DATA(2),
    /** Indicates this address was already trimmed.*/
    TRIM(3);

    private int id;
    OverwriteCause(int i) {
        this.id = i;
    }

    public static OverwriteCause fromId(int id) {
        for (OverwriteCause type : values()) {
            if (type.getId() == id) {
                return type;
            }
        }
        return null;
    }
}
