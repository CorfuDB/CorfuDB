package org.corfudb.util;

import org.corfudb.util.NodeLocator.NodeLocatorBuilder;
import org.corfudb.util.NodeLocator.Protocol;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class NodeLocatorTest {

    @Test
    public void equalsTest(){
        NodeLocatorBuilder builder = NodeLocator.builder()
                .host("a")
                .port(123);

        NodeLocator n1 = builder.option("opt1", "val1").build();
        NodeLocator n2 = builder.option("opt2", "val2").build();

        assertThat(n1).isEqualTo(n2);
    }

    @Test
    public void invalidNodeThrowsException() {
        assertThatThrownBy(() -> NodeLocator.parseString("invalid{}"))
            .isInstanceOf(IllegalArgumentException.class);
    }

    /** Tests that a legacy (without protocol) node parses correctly. **/
    @Test
    public void legacyNodeParses() {
        final int portNum = 3000;
        final String ipAddr = "10.0.0.1";
        NodeLocator locator = NodeLocator.parseString(ipAddr + ":" + portNum);

        assertThat(locator.getHost()).isEqualTo(ipAddr);
        assertThat(locator.getPort()).isEqualTo(portNum);
        assertThat(locator.getNodeId()).isNull();
        assertThat(locator.getProtocol()).isEqualTo(Protocol.TCP);
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
