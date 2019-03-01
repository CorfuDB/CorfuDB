package org.corfudb.util;

import com.google.common.collect.ImmutableMap;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.Singular;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * {@link NodeLocator}s represent locators for Corfu nodes.
 *
 * <p>A detailed document regarding their contents and format can be found in docs/NODE_FORMAT.md
 */
@Builder
@EqualsAndHashCode
public class NodeLocator implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Represents protocols for Corfu nodes.
     */
    public enum Protocol {
        /**
         * Default TCP-based protocol.
         */
        TCP
    }

    /**
     * The protocol to use.
     */
    @Getter
    @NonNull
    @Default
    private final Protocol protocol = Protocol.TCP;

    /**
     * The host the node is located on.
     */
    @Getter
    @NonNull
    private final String host;

    /**
     * The port number on the host the node is located on.
     */
    @Getter
    private final int port;

    /**
     * The ID of the node. Can be null if node id matching is not requested.
     */
    @Getter
    private final UUID nodeId;

    /**
     * A map of options.
     */
    @Singular
    private final ImmutableMap<String, String> options;

    /**
     * Parse a node locator string.
     *
     * @param toParse The string to parse.
     * @return A {@link NodeLocator} which represents the string.
     */
    public static NodeLocator parseString(String toParse) {
        try {
            // Fix a "legacy" node locator, which doesn't have a protocol.
            if (!toParse.contains("://")) {
                toParse = "tcp://" + toParse;
            }

            URI url = new URI(toParse);

            // Get the protocol from the enum
            Protocol proto = Protocol.valueOf(url.getScheme().toUpperCase());

            // Host/port are as in a URL
            String host = url.getHost();
            int port = url.getPort();

            // Node ID is from the path, if present.
            UUID nodeId;
            if (url.getPath().equals("") || url.getPath().equals("/")) {
                // No path, so nodeId is null
                nodeId = null;
            } else {
                nodeId = UuidUtils.fromBase64(url.getPath().replaceFirst("/", ""));
            }

            // Options map is from the query, if present.
            Map<String, String> options;
            if (url.getQuery() == null || url.getQuery().equals("")) {
                options = Collections.emptyMap();
            } else {
                String[] query = url.getQuery().split("&");
                options = Arrays.stream(query)
                        .map(keyValue -> keyValue.split("="))
                        .collect(Collectors.toMap(kv -> kv[0], kv -> kv[1]));
            }

            return NodeLocator.builder()
                    .protocol(proto)
                    .host(host)
                    .port(port)
                    .nodeId(nodeId)
                    .options(options)
                    .build();

        } catch (URISyntaxException m) {
            throw new IllegalArgumentException(m);
        }
    }

    public String toEndpointUrl(){
        return host + ":" + port;
    }

    @Override
    public String toString() {
        return toEndpointUrl();
    }
}
