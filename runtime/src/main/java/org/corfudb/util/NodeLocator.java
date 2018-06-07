package org.corfudb.util;

import com.google.common.collect.ImmutableMap;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.Builder;
import lombok.Data;
import lombok.Singular;

/** {@link NodeLocator}s represent locators for Corfu nodes.
 *
 *  <p>A detailed document regarding their contents and format can be found in docs/NODE_FORMAT.md
 *
 */
@Data
@Builder
public class NodeLocator implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Represents protocols for Corfu nodes. */
    public enum Protocol {
        TCP     /** Default TCP-based protocol. */
    }

    /** The protocol to use. */
    @Builder.Default private Protocol protocol = Protocol.TCP;

    /** The host the node is located on. */
    final String host;

    /** The port number on the host the node is located on. */
    final int port;

    /** The ID of the node. Can be null if node id matching is not requested. */
    @Builder.Default private UUID nodeId = null;

    /** A map of options. */
    @Singular final ImmutableMap<String, String> options;

    /** Parse a node locator string.
     *
     * @param toParse   The string to parse.
     * @return          A {@link NodeLocator} which represents the string.
     */
    public static NodeLocator parseString(String toParse) {
        try {
            // Fix a "legacy" node locator, which doesn't have a protocol.
            if (!toParse.contains("://")) {
                toParse = "tcp://" + toParse;
            }

            final URI url = new URI(toParse);

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


    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder()
                .append(protocol.toString().toLowerCase())
                .append("://")
                .append(host)
                .append(":")
                .append(port)
                .append("/");

        if (nodeId != null) {
            sb.append(UuidUtils.asBase64(nodeId));
        }

        if (!options.isEmpty()) {
            sb.append("?");
            sb.append(options.entrySet().stream()
                    .map(e -> e.getKey() + "=" + e.getValue())
                    .collect(Collectors.joining("&")));
        }

        return sb.toString();
    }


    /** Returns true if the provided string points to the same node.
     *
     * <p>A string points to the same node as this {@link NodeLocator} if
     * it has the same node ID, or it has no node ID and points to the same
     * host and port.
     *
     * @param nodeString    The string to check.
     * @return              True, if the string points to the node referenced by this {@link this}.
     * @throws IllegalArgumentException     If the provided string cannot be parsed as a
     *                                      {@link NodeLocator}.
     */
    public boolean isSameNode(@Nonnull String nodeString) {
        NodeLocator otherNode = NodeLocator.parseString(nodeString);
        // The nodes are the same if their Node IDs are the same.
        if (otherNode.getNodeId() != null && otherNode.getNodeId().equals(getNodeId())) {
            return true;
        } else {
            // Otherwise, the both node IDs must not be set
            // and must match by host and port.
            return !(otherNode.getNodeId() == null && getNodeId() != null)
                && otherNode.getHost().equals(getHost())
                && otherNode.getPort() == getPort();
        }
    }
}
