package org.corfudb.infrastructure;

import static org.assertj.core.api.Assertions.assertThatCode;

import org.junit.Test;

public class CorfuServerNodeTest {

  @Test
  public void nodeCloseTest() {
    final int port = 9000;
    ServerContext context = ServerContextBuilder.defaultContext(port);
    CorfuServerNode node = new CorfuServerNode(context);
    assertThatCode(node::close).doesNotThrowAnyException();
  }
}
