package org.corfudb.infrastructure.management;

import com.google.common.collect.ImmutableMap;
import org.assertj.core.api.Assertions;
import org.corfudb.runtime.clients.NettyClientRouter;
import org.junit.Test;

import java.util.Map;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class FailureDetectorTest {

    NettyClientRouter getTestRouter(long rpcTimeout, long connectionTimeout) {
          NettyClientRouter nettyClientRouter = mock(NettyClientRouter.class);
          doReturn(rpcTimeout).when(nettyClientRouter).getTimeoutResponse();
          doReturn(connectionTimeout).when(nettyClientRouter).getTimeoutConnect();
          return nettyClientRouter;

    }

    void assertRouterTimeouts(long maxSleepBetweenPolls, Map<String, NettyClientRouter> routers,
                              Map<String, Long> responseTimeouts, String endpoint) {
        long newResponseTimeout = responseTimeouts.get(endpoint);
        NettyClientRouter router =  routers.get(endpoint);
        Assertions.assertThat(router.getTimeoutConnect() + newResponseTimeout)
                .isLessThanOrEqualTo(maxSleepBetweenPolls);
    }

    @Test
    public void testRouterTimeouts() {
        long rpcTimeout = 5000;
        long connectionTimeout = 500;
        long maxSleepBetweenPolls = 2000;
        String server = "localhost";
        Map<String, NettyClientRouter> routers =
                ImmutableMap.of(server, getTestRouter(rpcTimeout, connectionTimeout));
        Map<String, Long> adjustedResponseTimeouts =
                FailureDetector.getAdjustedResponseTimeouts(routers, maxSleepBetweenPolls);
        assertRouterTimeouts(maxSleepBetweenPolls, routers, adjustedResponseTimeouts, server);

        rpcTimeout = 5000;
        connectionTimeout = 500;
        maxSleepBetweenPolls = 10000;
        routers = ImmutableMap.of(server, getTestRouter(rpcTimeout, connectionTimeout));
        adjustedResponseTimeouts = FailureDetector.getAdjustedResponseTimeouts(routers, maxSleepBetweenPolls);
        assertRouterTimeouts(maxSleepBetweenPolls, routers, adjustedResponseTimeouts, server);

        rpcTimeout = 2000;
        connectionTimeout = 3000;
        maxSleepBetweenPolls = 4000;
        routers = ImmutableMap.of(server, getTestRouter(rpcTimeout, connectionTimeout));
        adjustedResponseTimeouts = FailureDetector.getAdjustedResponseTimeouts(routers, maxSleepBetweenPolls);
        assertRouterTimeouts(maxSleepBetweenPolls, routers, adjustedResponseTimeouts, server);

        rpcTimeout = 2000;
        connectionTimeout = 3000;
        maxSleepBetweenPolls = 10000;
        routers = ImmutableMap.of(server, getTestRouter(rpcTimeout, connectionTimeout));
        adjustedResponseTimeouts = FailureDetector.getAdjustedResponseTimeouts(routers, maxSleepBetweenPolls);
        assertRouterTimeouts(maxSleepBetweenPolls, routers, adjustedResponseTimeouts, server);

        rpcTimeout = 5000;
        connectionTimeout = 1000;
        maxSleepBetweenPolls = 900;
        routers = ImmutableMap.of(server, getTestRouter(rpcTimeout, connectionTimeout));
        final long maxSleep = maxSleepBetweenPolls;
        final Map<String, NettyClientRouter> routers2 = routers;
        Assertions.assertThatThrownBy(() -> FailureDetector.getAdjustedResponseTimeouts(routers2, maxSleep))
                .isInstanceOf(IllegalArgumentException.class);

    }
}