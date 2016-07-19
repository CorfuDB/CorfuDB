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
    boolean single = true;
    boolean memory = true;
    String logPath = null;
    boolean sync = false;
    int maxCache = 1000000;
    int checkpoint = 100;
    String address = "localhost";
    int port = 9000;

    public ServerConfigBuilder() {

    }

    public Map<String, Object> build() {
        ImmutableMap.Builder<String,Object> builder =
                new ImmutableMap.Builder<String, Object>()
                .put("--initial-token", initialToken)
                .put("--single", single)
                .put("--memory", memory);
        if (logPath != null) {
         builder.put("--log-path", logPath);
        }
         builder
                .put("--sync", sync)
                .put("--max-cache", maxCache)
                .put("--checkpoint", checkpoint)
                .put("--address", address)
                .put("<port>", port);
        return builder.build();
    }

    public static Map<String,Object> defaultConfig(int port) {
        return new ServerConfigBuilder().setPort(port).build();
    }
}
