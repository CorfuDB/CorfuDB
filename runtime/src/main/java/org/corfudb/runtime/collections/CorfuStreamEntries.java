package org.corfudb.runtime.collections;

import lombok.Getter;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * A single corfu transaction can/may have updates to multiple tables.
 * Each table in CorfuStore is represented by a stream identified by a UUID.
 * This class returns those updates to the tables grouped by their stream UUIDs.
 * This class allows further expansion of the returned values.
 *
 * created by hisundar on 2019-10-22
 */
public class CorfuStreamEntries {
    @Getter
    private final Map<TableSchema, List<CorfuStreamEntry>> entries;

    public CorfuStreamEntries(Map<TableSchema, List<CorfuStreamEntry>> entries) {
        this.entries = entries;
    }
}
