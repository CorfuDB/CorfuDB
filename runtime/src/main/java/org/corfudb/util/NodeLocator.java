package org.corfudb.util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Data;
import lombok.NonNull;
import lombok.Singular;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/** {@link NodeLocator}s represent locators for Corfu nodes.
 *
 *  <p>A detailed document regarding their contents and format can be found in docs/NODE_FORMAT.md
 *
 */
@Data
@Builder
public class NodeLocator implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final UUID DEFAULT_NODE_ID = UUID.fromString("00000000-0000-0000-0000-000000000000");

    /**
     * Represents protocols for Corfu nodes.
     */
    public enum Protocol {
        /**
         * Default TCP-based protocol.
         */
        TCP
    }

    /** The protocol to use. */
    @Default
    @NonNull
    private Protocol protocol = Protocol.TCP;

    /** The host the node is located on. */
    @NonNull
    private final String host;

    /** The port number on the host the node is located on. */
    private final int port;

    /** The ID of the node. Can be null if node id matching is not requested. */
    @Default
    @NonNull
    private final UUID nodeId = DEFAULT_NODE_ID;

    /** A map of options. */
    @Singular
    final ImmutableMap<String, String> options;

    /**
     * Parse a node locator string.
     *
     * @param connectionString   The string to parse.
     * @return          A {@link NodeLocator} which represents the string.
     */
    public static NodeLocator parseString(String connectionString) {
        try {
            //Fix a "legacy" node locator, which doesn't have a protocol.
            if (!connectionString.contains("://")) {
                connectionString = "tcp://" + connectionString;
            }

            final URI url = new URI(connectionString);

            // Get the protocol from the enum
            Protocol proto = Protocol.valueOf(url.getScheme().toUpperCase());

            // Host/port are as in a URL
            String host = url.getHost();
            int port = url.getPort();

            // Node ID is from the path, if present.
            UUID nodeId = NodeLocator.DEFAULT_NODE_ID;
            if (!url.getPath().equals("") && !url.getPath().equals("/")) {
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

    public static ImmutableList<String> transformToStringsList(Collection<NodeLocator> nodes) {
        List<String> result = nodes.stream().map(NodeLocator::toEndpointUrl).collect(Collectors.toList());
        return ImmutableList.copyOf(result);
    }

    public static ImmutableSet<String> transformToStringsSet(Collection<NodeLocator> nodes) {
        List<String> result = nodes.stream().map(NodeLocator::toEndpointUrl).collect(Collectors.toList());
        return ImmutableSet.copyOf(result);
    }

    public static ImmutableList<NodeLocator> transformToList(Collection<String> nodes) {
        List<NodeLocator> result = nodes.stream().map(NodeLocator::parseString).collect(Collectors.toList());
        return ImmutableList.copyOf(result);
    }

    public static ImmutableSet<NodeLocator> transformToSet(Collection<String> nodes) {
        Set<NodeLocator> result = nodes.stream().map(NodeLocator::parseString).collect(Collectors.toSet());
        return ImmutableSet.copyOf(result);
    }

    public String toEndpointUrl(){
        StringBuilder sb = new StringBuilder()
                //TODO migrate to url format in future versions
                //.append(protocol.toString().toLowerCase())
                //.append("://")
                .append(host)
                .append(":")
                .append(port);
                //.append("/");

        return sb.toString();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(toEndpointUrl());

        sb.append(UuidUtils.asBase64(nodeId));

        if (!options.isEmpty()) {
            sb.append("?");
            sb.append(options.entrySet().stream()
                    .map(e -> e.getKey() + "=" + e.getValue())
                    .collect(Collectors.joining("&")));
        }

        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NodeLocator that = (NodeLocator) o;
        return port == that.port &&
                protocol == that.protocol &&
                Objects.equals(host, that.host);
    }

    @Override
    public int hashCode() {
        return Objects.hash(protocol, host, port);
    }
}
