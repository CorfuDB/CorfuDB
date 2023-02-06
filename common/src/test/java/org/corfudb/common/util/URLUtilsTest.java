package org.corfudb.common.util;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.net.InetSocketAddress;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

/**
 * Test class to test methods of {@link URLUtils} class.
 * Created by cgudisagar on 2/3/23.
 */
public class URLUtilsTest {
    private static final String IPV4_ADDRESS_1 = "127.0.0.1";
    private static final String IPV4_ADDRESS_2 = "127.0.0.2";
    private static final String IPV6_ADDRESS_1 = "[0:0:0:0:0:0:0:1]";
    private static final String IPV6_ADDRESS_2 = "[0:0:0:0:0:0:0:2]";
    private static final String IPV6_ADDRESS_1_SHORT = "[::1]";
    private static final String IPV6_ADDRESS_1_MALFORMED = "::1";
    private static final String IPV6_ADDRESS_2_SHORT = "[::2]";
    private static final String IPV6_ADDRESS_2_MALFORMED = "::2";
    private static final String LOCALHOST = "localhost";
    private static final String PORT_STRING_0 = "9000";
    private static final String PORT_STRING_1 = "9001";
    private static final int PORT_INT_9000 = 9000;
    private static final int PORT_INT_9001 = 9001;
    private static final String LOCALHOST_ENDPOINT_URL = "localhost:9000";
    private static final String IPV4_ENDPOINT_URL_1 = "127.0.0.1:9000";
    private static final String IPV4_ENDPOINT_URL_2 = "127.0.0.2:9001";
    private static final String IPV6_ENDPOINT_URL_1 = "[0:0:0:0:0:0:0:1]:9000";
    private static final String IPV6_ENDPOINT_URL_2 = "[0:0:0:0:0:0:0:2]:9001";
    private static final String IPV6_ENDPOINT_URL_1_SHORT = "[::1]:9000";
    private static final String IPV6_ENDPOINT_URL_3_SHORT_MALFORMED = "::1:9000";
    private static final String IPV6_ENDPOINT_URL_MALFORMED_1 = "0:0:0:0:0:0:0:1:9000";
    private static final String IPV6_ENDPOINT_URL_MALFORMED_2 = "0:0:0:0:0:0:0:2:9001";

    /**
     * Utility method to get a mock ChannelHandlerContext object.
     */
    private static ChannelHandlerContext getMockChannelHandlerContext(
            String localAddress, int localPort, String remoteAddress, int remotePort) {
        ChannelHandlerContext ctx;
        ctx = Mockito.mock(ChannelHandlerContext.class);
        Channel ch = Mockito.mock(Channel.class);
        when(ch.localAddress()).thenReturn(new InetSocketAddress(localAddress, localPort));
        when(ch.remoteAddress()).thenReturn(new InetSocketAddress(remoteAddress, remotePort));
        when(ctx.channel()).thenReturn(ch);
        return ctx;
    }

    /**
     * Test that version {@link URLUtils#getVersionFormattedEndpointURL} returns a version-formatted URL
     * that contains the formatted host address along with the port.
     */
    @Test
    void testGetVersionFormattedEndpointURL() {
        assertThat(URLUtils.getVersionFormattedEndpointURL(LOCALHOST, PORT_INT_9000))
                .isEqualTo(LOCALHOST_ENDPOINT_URL);
        assertThat(URLUtils.getVersionFormattedEndpointURL(LOCALHOST, PORT_STRING_0))
                .isEqualTo(LOCALHOST_ENDPOINT_URL);
        assertThat(URLUtils.getVersionFormattedEndpointURL(IPV4_ENDPOINT_URL_1))
                .isEqualTo(IPV4_ENDPOINT_URL_1);
        assertThat(URLUtils.getVersionFormattedEndpointURL(IPV6_ENDPOINT_URL_1))
                .isEqualTo(IPV6_ENDPOINT_URL_1);
        assertThat(URLUtils.getVersionFormattedEndpointURL(IPV6_ENDPOINT_URL_MALFORMED_1))
                .isEqualTo(IPV6_ENDPOINT_URL_1);
        assertThat(URLUtils.getVersionFormattedEndpointURL(IPV6_ENDPOINT_URL_MALFORMED_2))
                .isEqualTo(IPV6_ENDPOINT_URL_2);
        assertThat(URLUtils.getVersionFormattedEndpointURL(IPV6_ENDPOINT_URL_3_SHORT_MALFORMED))
                .isEqualTo(IPV6_ENDPOINT_URL_1_SHORT);
    }

    /**
     * Test that version {@link URLUtils#getVersionFormattedHostAddress} returns a version-formatted URL
     * that contains the formatted host address.
     */
    @Test
    void testGetVersionFormattedHostAddress() {
        // Should not alter IPv4 Addresses
        assertThat(URLUtils.getVersionFormattedHostAddress(IPV4_ADDRESS_1))
                .isEqualTo(IPV4_ADDRESS_1);

        // Should not alter IPv4 Addresses
        assertThat(URLUtils.getVersionFormattedHostAddress(IPV4_ADDRESS_2))
                .isEqualTo(IPV4_ADDRESS_2);

        // Should not alter version-formatted IPv6 Address
        assertThat(URLUtils.getVersionFormattedHostAddress(IPV6_ADDRESS_1_SHORT))
                .isEqualTo(IPV6_ADDRESS_1_SHORT);

        // Should not alter version-formatted IPv6 Address
        assertThat(URLUtils.getVersionFormattedHostAddress(IPV6_ADDRESS_2_SHORT))
                .isEqualTo(IPV6_ADDRESS_2_SHORT);

        // Should not alter version-formatted IPv6 Address
        assertThat(URLUtils.getVersionFormattedHostAddress(LOCALHOST))
                .isEqualTo(LOCALHOST);

        // Should return version-formatted IPv6 Address with '[' and ']'
        assertThat(URLUtils.getVersionFormattedHostAddress(IPV6_ADDRESS_1_MALFORMED))
                .isEqualTo(IPV6_ADDRESS_1_SHORT);

        // Should return version-formatted IPv6 Address with '[' and ']'
        assertThat(URLUtils.getVersionFormattedHostAddress(IPV6_ADDRESS_2_MALFORMED))
                .isEqualTo(IPV6_ADDRESS_2_SHORT);
    }

    /**
     * Test that {@link URLUtils#getLocalEndpointFromCtx} returns the endpoint containing
     * the local socket address extracted from the netty Ctx
     */
    @Test
    void testGetLocalEndpointFromCtx() {
        ChannelHandlerContext ctx =
                getMockChannelHandlerContext(IPV4_ADDRESS_1, PORT_INT_9000, IPV4_ADDRESS_1, PORT_INT_9000);
        assertThat(URLUtils.getLocalEndpointFromCtx(ctx)).isEqualTo(IPV4_ENDPOINT_URL_1);

        // IPv6
        ctx = getMockChannelHandlerContext(IPV6_ADDRESS_1_SHORT, PORT_INT_9000, IPV6_ADDRESS_2_SHORT, PORT_INT_9001);
        assertThat(URLUtils.getLocalEndpointFromCtx(ctx)).isEqualTo(IPV6_ENDPOINT_URL_1);
    }

    /**
     * Test that {@link URLUtils#getRemoteEndpointFromCtx} returns the endpoint containing
     * the remote socket address extracted from the netty Ctx
     */
    @Test
    void getRemoteEndpointFromCtx() {
        ChannelHandlerContext ctx =
                getMockChannelHandlerContext(IPV4_ADDRESS_1, PORT_INT_9000, IPV4_ADDRESS_2, PORT_INT_9001);
        assertThat(URLUtils.getRemoteEndpointFromCtx(ctx)).isEqualTo(IPV4_ENDPOINT_URL_2);

        // IPv6
        ctx = getMockChannelHandlerContext(IPV6_ADDRESS_1_SHORT, PORT_INT_9000, IPV6_ADDRESS_2_SHORT, PORT_INT_9001);
        assertThat(URLUtils.getRemoteEndpointFromCtx(ctx)).isEqualTo(IPV6_ENDPOINT_URL_2);
    }

    /**
     * Test that {@link URLUtils#getPortFromEndpointURL} extracts the port from
     * the IPV4 or IPV6 endpoint URL
     */
    @Test
    void testGetPortFromEndpointURL() {
        assertThat(URLUtils.getPortFromEndpointURL(IPV4_ENDPOINT_URL_1)).isEqualTo(PORT_STRING_0);
        assertThat(URLUtils.getPortFromEndpointURL(IPV4_ENDPOINT_URL_2)).isEqualTo(PORT_STRING_1);
        assertThat(URLUtils.getPortFromEndpointURL(IPV6_ENDPOINT_URL_1)).isEqualTo(PORT_STRING_0);
        assertThat(URLUtils.getPortFromEndpointURL(IPV6_ENDPOINT_URL_2)).isEqualTo(PORT_STRING_1);
        assertThat(URLUtils.getPortFromEndpointURL(IPV6_ENDPOINT_URL_1_SHORT)).isEqualTo(PORT_STRING_0);
        // no colon
        assertThat(URLUtils.getPortFromEndpointURL(IPV4_ADDRESS_1)).isEqualTo(IPV4_ADDRESS_1);
        assertThat(URLUtils.getPortFromEndpointURL(PORT_STRING_0)).isEqualTo(PORT_STRING_0);
    }

    /**
     * Test that {@link URLUtils#getPortFromEndpointURL} extracts the port from
     * the IPV4 or IPV6 endpoint URL
     */
    @Test
    void testGetHostFromEndpointURL() {
        assertThat(URLUtils.getHostFromEndpointURL(IPV4_ENDPOINT_URL_1)).isEqualTo(IPV4_ADDRESS_1);
        assertThat(URLUtils.getHostFromEndpointURL(IPV4_ENDPOINT_URL_2)).isEqualTo(IPV4_ADDRESS_2);
        assertThat(URLUtils.getHostFromEndpointURL(IPV6_ENDPOINT_URL_1)).isEqualTo(IPV6_ADDRESS_1);
        assertThat(URLUtils.getHostFromEndpointURL(IPV6_ENDPOINT_URL_2)).isEqualTo(IPV6_ADDRESS_2);
        assertThat(URLUtils.getHostFromEndpointURL(IPV6_ENDPOINT_URL_1_SHORT)).isEqualTo(IPV6_ADDRESS_1_SHORT);
        // no colon
        assertThat(URLUtils.getPortFromEndpointURL(IPV4_ADDRESS_1)).isEqualTo(IPV4_ADDRESS_1);
        assertThat(URLUtils.getPortFromEndpointURL(PORT_STRING_0)).isEqualTo(PORT_STRING_0);
    }
}
