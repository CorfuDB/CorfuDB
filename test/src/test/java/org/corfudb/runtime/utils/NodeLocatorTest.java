package org.corfudb.runtime.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.util.NodeLocator;
import org.corfudb.util.NodeLocator.Protocol;
import org.junit.Test;

public class NodeLocatorTest extends AbstractViewTest {

    @Test
    public void invalidNodeThrowsException() {
        assertThatThrownBy(() -> NodeLocator.parseString("invalid{}"))
            .isInstanceOf(IllegalArgumentException.class);
    }

    /** Tests that a legacy (without protocol) node parses correctly. **/
    @Test
    public void legacyNodeParses() {
        final int PORT_NUM = 3000;
        NodeLocator locator = NodeLocator.parseString("10.0.0.1:3000");

        assertThat(locator.getHost())
            .isEqualTo("10.0.0.1");

        assertThat(locator.getPort())
            .isEqualTo(PORT_NUM);

        assertThat(locator.getNodeId())
            .isNull();

        assertThat(locator.getProtocol())
            .isEqualTo(Protocol.TCP);
    }

    @Test
    public void nodeCanBeConvertedBackAndForth() {
        NodeLocator locator = NodeLocator.builder()
                .host("localhost")
                .port(1)
                .protocol(Protocol.TCP)
                .build();
        NodeLocator parsed = NodeLocator.parseString(locator.toEndpointUrl());

        assertThat(locator).isEqualToComparingFieldByField(parsed);
        assertThat(locator).isEqualTo(parsed);
    }

    @Test
    public void nodeCanBeConvertedBackAndForthWithNoNodeId() {
        NodeLocator locator = NodeLocator.builder()
            .host("localhost")
            .port(1)
            .nodeId(null)
            .protocol(Protocol.TCP)
            .build();

        NodeLocator parsed = NodeLocator.parseString(locator.toString());

        assertThat(locator).isEqualToComparingFieldByField(parsed);
        assertThat(locator).isEqualTo(parsed);
    }

}
