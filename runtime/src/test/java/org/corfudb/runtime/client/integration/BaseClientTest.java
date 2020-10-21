package org.corfudb.runtime.client.integration;

import io.netty.channel.nio.NioEventLoopGroup;
import org.assertj.core.api.Assertions;
import org.corfudb.runtime.RuntimeParameters;
import org.corfudb.runtime.clients.BaseClient;
import org.corfudb.runtime.clients.NettyClientRouter;
import org.corfudb.runtime.proto.service.CorfuMessage;
import org.corfudb.util.NodeLocator;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static java.lang.Thread.sleep;

@Ignore("Required only for integration Tests with a Corfu server running on port 9000")
public class BaseClientTest {

    private BaseClient baseClient;

    @Before
    public void setup(){
        NodeLocator nodeLocator = NodeLocator.parseString("localhost:9000");
        UUID clientID = UUID.fromString("00000000-0000-0000-0000-000000000000");
        RuntimeParameters runtimeParameters = new RuntimeParameters();
        runtimeParameters.setClientId(clientID);
        runtimeParameters.setCustomNettyChannelOptions(RuntimeParameters.DEFAULT_CHANNEL_OPTIONS);
        runtimeParameters.setConnectionTimeout(Duration.ofHours(1));
        runtimeParameters.setRequestTimeout(Duration.ofHours(1));
        runtimeParameters.setIdleConnectionTimeout(10000000);
        NettyClientRouter ncr = new NettyClientRouter(nodeLocator,
                new NioEventLoopGroup(),
                runtimeParameters);
        this.baseClient = new BaseClient(ncr, 0L, clientID);
    }

    @Test
    public void pingTest() {
        Assertions.assertThat(baseClient.pingProto().join().booleanValue()).isTrue();
    }
}
