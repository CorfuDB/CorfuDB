package org.corfudb.generator.replayer.replayOperations;

import com.google.common.reflect.TypeToken;
import lombok.Getter;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.SMRMap;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by Sam Behnam on 2/12/18.
 */
public class Configuration {

    @Getter
    final CorfuRuntime corfuRuntime;

    @Getter
    final Map<String, Map<String, Object>> maps;

    public Configuration(CorfuRuntime corfuRuntime, Set<String> streamIdSet) {
        this.corfuRuntime = corfuRuntime;

        // Set up simulated maps for the ones used during recording
        maps = new HashMap<>();
        for (String streamId : streamIdSet) {
            SMRMap<String, Object> smrMap = corfuRuntime
                    .getObjectsView()
                    .build()
                    .setStreamName(streamId)
                    .setTypeToken(new TypeToken<SMRMap<String, Object>>() {})
                    .open();
            maps.put(streamId, smrMap);
        }
    }

    public Map<String, Object> getMap(String streamId) {
        return  maps.get(streamId);
    }
}
