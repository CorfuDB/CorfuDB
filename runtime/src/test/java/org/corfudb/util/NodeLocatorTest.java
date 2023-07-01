package org.corfudb.util;

import org.corfudb.util.NodeLocator.Protocol;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class NodeLocatorTest {

    private static final String LOCALHOST = "localhost";

    @Test
    public void invalidNodeThrowsException() {
        assertThatThrownBy(() -> NodeLocator.parseString("invalid{}"))
            .isInstanceOf(IllegalArgumentException.class);
    }

    /** Tests that a legacy (without protocol) node parses correctly. **/
    @Test
    public void legacyNodeParses() {
        final int port = 3000;
        NodeLocator locator = NodeLocator.parseString("10.0.0.1:3000");

        assertThat(locator.getHost())
            .isEqualTo("10.0.0.1");

        assertThat(locator.getPort())
            .isEqualTo(port);

        assertThat(locator.getNodeId())
            .isNull();

        assertThat(locator.getProtocol())
            .isEqualTo(Protocol.TCP);
    }

    @Test
    public void nodeCanBeConvertedBackAndForth() {
        NodeLocator locator = NodeLocator.builder()
                .host(LOCALHOST)
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
            .host(LOCALHOST)
            .port(1)
            .nodeId(null)
            .protocol(Protocol.TCP)
            .build();

        NodeLocator parsed = NodeLocator.parseString(locator.toString());

        assertThat(locator).isEqualToComparingFieldByField(parsed);
        assertThat(locator).isEqualTo(parsed);
    }

    /**
     * Test that {@link NodeLocator#toEndpointUrl()} method formats the endpoints
     * of IPv4 and IPv6 versions correctly.
     */
    @Test
    public void testToEndpointURL() {
        testToEndpointURLHelper("::1");
        testToEndpointURLHelper("[::1]");
        testToEndpointURLHelper("0:0:0:0:0:0:0:1");
        testToEndpointURLHelper("[0:0:0:0:0:0:0:1]");
        testToEndpointURLHelper(LOCALHOST);
        testToEndpointURLHelper("127.0.0.1");
    }

    public void testToEndpointURLHelper(String host) {
        NodeLocator locator = NodeLocator.builder()
                .host(host)
                .port(9000)
                .build();

        String toEndpointUrl = locator.toEndpointUrl();
        NodeLocator parsed = NodeLocator.parseString(locator.toEndpointUrl());

        assertThat(locator.toEndpointUrl()).isEqualTo(parsed.toEndpointUrl());
        assertThat(locator.toString()).isEqualTo(toEndpointUrl);
    }

}
