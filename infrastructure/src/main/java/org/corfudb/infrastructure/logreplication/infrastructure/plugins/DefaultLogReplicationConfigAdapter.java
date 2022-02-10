package org.corfudb.infrastructure.logreplication.infrastructure.plugins;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.TableRegistry;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Default testing implementation of a Log Replication Config Provider
 *
 * This implementation retrieves a fixed set of tables, which are used for testing purposes.
 */
public class DefaultLogReplicationConfigAdapter implements ILogReplicationConfigAdapter {

    private Set<String> streamsToReplicate;

    public static final int MAP_COUNT = 10;
    public static final String TABLE_PREFIX = "Table00";
    public static final String NAMESPACE = "LR-Test";
    public static final String TAG_ONE = "tag_one";
    private final int indexOne = 1;
    private final int indexTwo = 2;

    public DefaultLogReplicationConfigAdapter() {
        streamsToReplicate = new HashSet<>();
        streamsToReplicate.add("Table001");
        streamsToReplicate.add("Table002");
        streamsToReplicate.add("Table003");

        // Support for UFO
        for (int i = 1; i <= MAP_COUNT; i++) {
            streamsToReplicate.add(NAMESPACE + "$" + TABLE_PREFIX + i);
        }
    }

    @Override
    public String getVersion() {
        return "version_latest";
    }

    @Override
    public Map<UUID, List<UUID>> getStreamingConfigOnSink() {
        Map<UUID, List<UUID>> streamsToTagsMaps = new HashMap<>();
        UUID streamTagOneDefaultId = TableRegistry.getStreamIdForStreamTag(NAMESPACE, TAG_ONE);
        streamsToTagsMaps.put(CorfuRuntime.getStreamID(NAMESPACE + "$" + TABLE_PREFIX + indexOne),
                Collections.singletonList(streamTagOneDefaultId));
        streamsToTagsMaps.put(CorfuRuntime.getStreamID(NAMESPACE + "$" + TABLE_PREFIX + indexTwo),
                Collections.singletonList(streamTagOneDefaultId));
        return streamsToTagsMaps;
    }
}
