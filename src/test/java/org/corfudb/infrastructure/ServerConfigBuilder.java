package org.corfudb.infrastructure;

import com.google.common.collect.ImmutableMap;
import lombok.Data;
import lombok.experimental.Accessors;

import java.util.Map;

/**
 * Created by mwei on 6/29/16.
 */
@Accessors(chain = true)
@Data
public class ServerConfigBuilder {

    long initialToken = 0;
    boolean single = false;
    boolean memory = true;
    String logPath = "";
    boolean sync = false;
    int maxCache = 1000000;
    int checkpoint = 100;

    public ServerConfigBuilder() {

    }

    public Map<String, Object> build() {
        return new ImmutableMap.Builder<String, Object>()
                .put("--initial-token", initialToken)
                .put("--single", single)
                .put("--memory", memory)
                .put("--log-path", logPath)
                .put("--sync", sync)
                .put("--max-cache", maxCache)
                .put("--checkpoint", checkpoint)
                .build();
    }
}
