package org.corfudb.runtime;

public interface TransmitterReplicationModule {
    /**
     * Full state data is requested for the application.
     * It is expected that this call is non-blocking and data will be provided in different thread.
     *
     * @param context replication context
     */
    void provideFullStateData(FullStateReplicationContext context);
}
