package org.corfudb.infrastructure;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CompletableFuture;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.junit.Test;

/** Created by mwei on 12/14/15. */
public class BaseServerTest extends AbstractServerTest {

  BaseServer bs;

  @Override
  public AbstractServer getDefaultServer() {
    if (bs == null) {
      bs = new BaseServer(ServerContextBuilder.defaultTestContext(0));
    }
    return bs;
  }

  @Test
  public void testPing() {
    CompletableFuture<Boolean> res = sendRequest(new CorfuMsg(CorfuMsgType.PING));
    assertThat(res.join()).isTrue();
  }
}
