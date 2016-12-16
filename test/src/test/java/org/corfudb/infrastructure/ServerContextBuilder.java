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

    long initialToken = 0;
    boolean single = true;
    boolean memory = true;
    String logPath = null;
    boolean noVerify = false;
    int maxCache = 1000000;
    int checkpoint = 100;
    String address = "test";
    int port = 9000;
    IServerRouter serverRouter;

    public ServerContextBuilder() {

    }

    public ServerContext build() {
        ImmutableMap.Builder<String,Object> builder =
                new ImmutableMap.Builder<String, Object>()
                .put("--initial-token", initialToken)
                .put("--single", single)
                .put("--memory", memory);
        if (logPath != null) {
         builder.put("--log-path", logPath);
        }
         builder
                .put("--no-verify", noVerify)
                .put("--max-cache", maxCache)
                .put("--checkpoint", checkpoint)
                .put("--address", address)
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
