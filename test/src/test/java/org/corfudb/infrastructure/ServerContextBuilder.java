package org.corfudb.infrastructure;

import com.google.common.collect.ImmutableMap;
import lombok.Data;
import lombok.experimental.Accessors;

/**
 * Created by mwei on 6/29/16.
 */
@Accessors(chain = true)
@Data
// Disable magic number check to make defaults readable
@SuppressWarnings("checkstyle:magicnumber")
public class ServerContextBuilder {

    long initialToken = 0L; // for testing, we want to reset the sequencer on each test

    boolean single = true;
    boolean memory = true;
    String logPath = null;
    boolean noVerify = false;
    boolean tlsEnabled = false;
    String cacheSizeHeapRatio = "0.5";
    String address = "test";
    int port = 9000;
    String seqCache = "1000";
    String managementBootstrapEndpoint = null;
    IServerRouter serverRouter;

    public ServerContextBuilder() {

    }

    public ServerContext build() {
        ImmutableMap.Builder<String,Object> builder =
                new ImmutableMap.Builder<String, Object>()
                .put("--initial-token", initialToken)
                .put("--single", single)
                .put("--memory", memory)
                .put("--sequencer-cache-size", seqCache);
        if (logPath != null) {
         builder.put("--log-path", logPath);
        }
        if (managementBootstrapEndpoint != null) {
            builder.put("--management-server", managementBootstrapEndpoint);
        }
         builder
                 .put("--no-verify", noVerify)
                 .put("--address", address)
                 .put("--cache-heap-ratio", cacheSizeHeapRatio)
                 .put("--enable-tls", tlsEnabled)
                 .put("<port>", port);
        return new ServerContext(builder.build(), serverRouter);
    }

    public static ServerContext defaultContext(int port) {
        return new ServerContextBuilder().setPort(port).build();
    }

    public static ServerContext emptyContext() {
        return new ServerContextBuilder().build();
    }

}
