package org.corfudb.infrastructure;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThatCode;


public class CorfuServerNodeTest {

    @Test
    public void nodeCloseTest() {
        final int port = 9000;
        ServerContext context = ServerContextBuilder.defaultContext(port);
        CorfuServerNode node = new CorfuServerNode(context);
        assertThatCode(node::close).doesNotThrowAnyException();
    }
}
