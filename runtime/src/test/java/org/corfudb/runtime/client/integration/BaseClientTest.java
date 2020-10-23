package org.corfudb.runtime.client.integration;

import io.netty.channel.nio.NioEventLoopGroup;
import org.assertj.core.api.Assertions;
import org.corfudb.protocols.wireprotocol.VersionInfo;
import org.corfudb.runtime.RuntimeParameters;
import org.corfudb.runtime.clients.BaseClient;
import org.corfudb.runtime.clients.NettyClientRouter;
import org.corfudb.util.NodeLocator;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.time.Duration;
import java.util.UUID;


@Ignore("Required only for integration Tests with a Corfu server running on port 9000")
public class BaseClientTest {

    private BaseClient baseClient;

    @Before
    public void setup() {
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
        Assertions.assertThat(baseClient.protoPing().join().booleanValue()).isTrue();
    }

    @Test
    public void restartTest() {
        Assertions.assertThat(baseClient.protoRestart().join().booleanValue()).isTrue();
    }

    @Test
    public void resetTest() {
        Assertions.assertThat(baseClient.protoReset().join().booleanValue()).isTrue();
    }

    @Test
    public void sealTest() {
        final long NEW_EPOCH = 1L;
        Assertions.assertThat(baseClient.protoSeal(NEW_EPOCH).join().booleanValue()).isTrue();
    }

    @Test
    public void versionTest() {
        VersionInfo viCorfuMsg = baseClient.getVersionInfo().join();
        VersionInfo viProtoMsg = baseClient.protoGetVersionInfo().join();
        Assertions.assertThat(viCorfuMsg.getJvmUsed()).isEqualTo(viProtoMsg.getJvmUsed());
        Assertions.assertThat(viCorfuMsg.getStartupArgs()).isEqualTo(viProtoMsg.getStartupArgs());
        Assertions.assertThat(viCorfuMsg.getVersion()).isEqualTo(viProtoMsg.getVersion());
        Assertions.assertThat(viCorfuMsg.getNodeId()).isEqualTo(viProtoMsg.getNodeId());
    }
}
