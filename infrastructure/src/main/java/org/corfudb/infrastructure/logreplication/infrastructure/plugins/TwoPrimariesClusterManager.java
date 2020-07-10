package org.corfudb.infrastructure.logreplication.infrastructure.plugins;


public class TwoPrimariesClusterManager extends FileReaderClusterManager {

    @Override
    String getConfigFilePath() {
        return "corfu_replication_config_two_primaries.properties";
    }
}
