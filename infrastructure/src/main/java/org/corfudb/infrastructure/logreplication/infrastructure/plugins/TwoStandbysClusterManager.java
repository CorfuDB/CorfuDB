package org.corfudb.infrastructure.logreplication.infrastructure.plugins;

public class TwoStandbysClusterManager extends FileReaderClusterManager {
    @Override
    String getConfigFilePath() {
        return "corfu_replication_config_two_standbys.properties";
    }
}
