package org.corfudb.runtime.clients;

import org.corfudb.infrastructure.ServerContextBuilder;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by rmichoud on 7/15/17.
 */
public class NettyClientRouterTest extends NettyCommTest {
    @Test
    public void doesNotUpdateEpochBackward() throws Exception{

        runWithBaseServer(
                (port) -> {
                    return new NettyServerData(ServerContextBuilder.defaultContext(port));
                },
                (port) -> {
                    return new NettyClientRouter("localhost", port);
                },
                (r, d) -> {
                    long currentEpoch = r.getEpoch();
                    r.setEpoch(currentEpoch+1);
                    assertThat(r.getEpoch()).isEqualTo(currentEpoch+1);

                    currentEpoch = r.getEpoch();

                    r.setEpoch(currentEpoch-1);
                    assertThat(r.getEpoch()).isNotEqualTo(currentEpoch-1).isEqualTo(currentEpoch);
                });
    }
}
