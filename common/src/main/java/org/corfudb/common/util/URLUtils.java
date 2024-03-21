package org.corfudb.common.util;

import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.conn.util.InetAddressUtils;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.function.Predicate;
import java.util.regex.Pattern;

/**
 * Contains Utility methods to work with URIs or the Socket Addresses.
 * Supports both IPv4 and IPv6 methods.
 *
 * Created by cgudisagar on 1/22/23.
 */
@Slf4j
public final class URLUtils {

    private static final String COLON_SEPERATOR = ":";

    // Matches things like "corfu" or "log-replication-0"
    private static final Predicate<String> SAAS_PATTERN1 = Pattern.compile("^[a-z-]+(-\\d+)?$").asPredicate();

    // Matches things like "corfu-0.corfu-headless.dev-env-23.svc.cluster.local"
    private static final Predicate<String> SAAS_PATTERN2 = Pattern.compile("^[a-z0-9-]+(-\\d+)?.[a-z0-9-]+.[a-z0-9-]+.svc.cluster.local$").asPredicate();

    private static final Predicate<String> SAAS_PATTERN = SAAS_PATTERN1.or(SAAS_PATTERN2);

    private URLUtils() {
        // prevent instantiation of this class
    }

    public static boolean hostMatchesSaasPattern(String host) {
        return SAAS_PATTERN.test(host);
    }

    /**
     * Returns a version-formatted URL that contains the formatted host address along with the port.
     *
     * @param host host address that needs formatting
     * @param port port of the endpoint
     * @return a version-formatted endpoint
     */
    public static String getVersionFormattedEndpointURL(String host, Integer port) {
        return getVersionFormattedEndpointURL(host, port.toString());
    }

    /**
     * Returns a version-formatted URL that contains the formatted host address along with the port.
     *
     * @param host host address that needs formatting
     * @param port port of the endpoint
     * @return a version-formatted endpoint
     */
    public static String getVersionFormattedEndpointURL(String host, String port) {
        return getVersionFormattedHostAddress(host) +
                COLON_SEPERATOR +
                port;
    }

    /**
     * Returns a version-formatted URL that contains the formatted host address along with the port.
     *
     * @param address host address that needs formatting
     * @return a version-formatted endpoint
     */
    public static String getVersionFormattedEndpointURL(String address) {
        int lastColonIndex = address.lastIndexOf(COLON_SEPERATOR);
        // if ':' is present, return a substring
        if (lastColonIndex != -1) {
            String host = address.substring(0, lastColonIndex);
            return getVersionFormattedHostAddress(host) +
                    address.substring(lastColonIndex);
        } else {
            log.warn("getVersionFormattedEndpointURL: Could not find colon in the address '{}'.", address);
            return address;
        }
    }

    /**
     * Returns a version-formatted URL that contains the formatted host address.
     * If host matches saas pattern, returns as is.
     *
     * @param host host address that needs formatting
     * @return version-formatted address
     * @throws IllegalArgumentException if the parameter host is null or empty
     */
    public static String getVersionFormattedHostAddress(String host) {

        // In LR-SaaS, host could be null.
        if (host == null || host.isEmpty()) {
            throw new IllegalArgumentException("getVersionFormattedHostAddress: " +
                    "host is either null or empty.");
        }

        // getByName(host) fails when host has scope/interface in it like ([....%eth0])
        // remove the trailing names %eth0 or %en0 if present
        String formattedHost = host.trim().split("%")[0];

        try {
            if (hostMatchesSaasPattern(formattedHost)) {
                return formattedHost;
            } else if (InetAddressUtils.isIPv6Address(InetAddress.getByName(formattedHost).getHostAddress())
                && formattedHost.charAt(0)!='[' && formattedHost.charAt(formattedHost.length()-1)!=']') {
                formattedHost = '[' + formattedHost + ']';
            }
        } catch (UnknownHostException e) {
            log.warn("Unable to validate the host address: " + formattedHost, e);
            return host;
        }

        return formattedHost;
    }

    /**
     * Return the local socket address extracted from the netty Ctx
     *
     * @param ctx ChannelHandlerContext
     * @return string in the form of IP:PORT
     */
    public static String getLocalEndpointFromCtx(ChannelHandlerContext ctx) {
        try {
            return getVersionFormattedHostAddress(((InetSocketAddress) ctx.channel().localAddress()).getAddress().getHostAddress())
                    + COLON_SEPERATOR
                    + ((InetSocketAddress) ctx.channel().localAddress()).getPort();
        } catch (NullPointerException ex) {
            return "unavailable";
        }
    }

    /**
     * Return the remote socket address extracted from the netty Ctx
     *
     * @param ctx ChannelHandlerContext
     * @return string in the form of IP:PORT
     */
    public static String getRemoteEndpointFromCtx(ChannelHandlerContext ctx) {
        try {
            return getVersionFormattedHostAddress(
                    ((InetSocketAddress) ctx.channel().remoteAddress()).getAddress().getHostAddress()
            )
                    + COLON_SEPERATOR
                    + ((InetSocketAddress) ctx.channel().remoteAddress()).getPort();
        } catch (NullPointerException ex) {
            return "unavailable";
        }
    }

    /**
     * Extracts the host from the IPV4 or IPV6 endpoint URL
     *
     * @param address IPV4 or IPV6 endpoint URL
     * @return extracted host address
     */
    public static String getHostFromEndpointURL(String address) {
        return extractionHelper(true, address);
    }

    /**
     * Extracts the port from the IPV4 or IPV6 endpoint URL
     *
     * @param address IPV4 or IPV6 endpoint URL
     * @return extracted port
     */
    public static String getPortFromEndpointURL(String address) {
        return extractionHelper(false, address);
    }

    /**
     * A helper method to extract host and ports
     * @param extractHost true to extract a host, false for port
     * @param address address to extract form
     * @return the extracted result
     */
    private static String extractionHelper(boolean extractHost, String address) {
        int lastColonIndex = address.lastIndexOf(COLON_SEPERATOR);
        // if ':' is present, return a substring
        if (lastColonIndex != -1) {
            if (extractHost) {
                return address.substring(0, lastColonIndex);
            } else {
                return address.substring(lastColonIndex + 1);
            }
        } else {
            log.warn("extractionHelper: Could not find colon in the address '{}'.", address);
            return address;
        }
    }

    public static enum NetworkInterfaceVersion {
        IPV4,
        IPV6
    }

}
