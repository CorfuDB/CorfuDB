package org.corfudb.infrastructure.logreplication.infrastructure.plugins;

/**
 * A file reader cluster manager that provides the following static topology:
 * - 1 active corfu node
 * - 2 active LRs
 * - 1 standby corfu node
 * - 1 standby LR
 */
public class TwoActivesClusterManager extends FileReaderClusterManager {

    @Override
    String getConfigFilePath() {
        return "corfu_replication_config_two_actives.properties";
    }
}
