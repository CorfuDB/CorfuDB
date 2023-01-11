package org.corfudb.infrastructure.logreplication.replication;

/**
 * Log replication metadata manager adapter to support rolling upgrades between LR-Version 1 and Version 2
 *
 * This class represents a thin transformation layer between both versions depending on the current versions running
 * in the system.
 *
 * Note: LR Version 2 has major schema changes for metadata tables.
 */
public class MetadataManagerAdapter {

    /**
     * Constructor
     */
    public MetadataManagerAdapter() {

    }


}
