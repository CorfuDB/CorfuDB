package org.corfudb.runtime.collections;

import lombok.Getter;
import org.corfudb.runtime.CorfuStoreMetadata.Timestamp;

import java.util.List;
import java.util.Map;

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

    /*
     Represents the timestamp for the retrieved entries. In the event of failures,
     clients can re-subscribe starting from this timestamp, in order to avoid data loss or
     the need to re-sync all the data.
     */
    @Getter
    private final Timestamp timestamp;

    public CorfuStreamEntries(Map<TableSchema, List<CorfuStreamEntry>> entries, Timestamp timestamp) {
        this.entries = entries;
        this.timestamp = timestamp;
    }
}
