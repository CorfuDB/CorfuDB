package org.corfudb.runtime.exceptions;

import lombok.Getter;

@Getter
public enum OverwriteCause {
    /** Indicates this address has been already written by a hole.*/
    HOLE(0),
    /** Indicates this address has been already written and it is 'potentially' the same data
     * (i.e., based on data length). The client will drive final verification. This intends to be
     * a hint so verification is only driven in very specific cases.*/
    SAME_DATA(1),
    /** Indicates this address has been already written and it is a different data (derived from the length).*/
    DIFF_DATA(2),
    /** Indicates this address was already trimmed.*/
    TRIM(3),
    /** Indicates there is no actual cause for the overwrite, hence no overwrite ecception has occured. */
    NONE(4);

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
