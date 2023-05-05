package org.corfudb.infrastructure.logreplication.infrastructure.plugins;

import org.apache.commons.lang3.tuple.Pair;

/**
 * Log replication allows for any transport layer to be plugged in.
 * This interface must be implemented to give the name of the property, in the plugin config file, which
 */
public interface LogReplicationTransportSelectorPlugin {

    Pair<String, String> getTransportPropertyToReadFromConfig();
}
