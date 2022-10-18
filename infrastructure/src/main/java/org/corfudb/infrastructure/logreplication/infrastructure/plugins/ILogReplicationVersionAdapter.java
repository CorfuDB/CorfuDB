package org.corfudb.infrastructure.logreplication.infrastructure.plugins;

/**
 * This Interface must be implemented by any external
 * provider to give the System's version
 */
public interface ILogReplicationVersionAdapter {

    /**
     * Returns a version string that indicates the version of LR.
     *
     * @return Version string that indicates the version of LR.
     */
    String getVersion();
}
