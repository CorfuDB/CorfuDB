package org.corfudb;

import org.corfudb.util.NodeLocator;

/** This class provides constants to refer to test servers.
 * Created by mwei on 12/9/16.
 */
public class CorfuTestServers {
    public static final String HOST_NAME = "test";

    /** The port for test server 0. */
    private final int PORT_0 = 0;

    /** The port for test server 1. */
    private final int PORT_1 = 1;

    /** The port for test server 2. */
    private final int PORT_2 = 2;

    /** The port for test server 3. */
    private final int PORT_3 = 3;

    /** The port for test server 4. */
    private final int PORT_4 = 4;

    /** The port for test server 5. */
    private final int PORT_5 = 5;

    /** The port for test server 6. */
    private final int PORT_6 = 6;

    /** The port for test server 7. */
    private final int PORT_7 = 7;

    /** The port for test server 8. */
    private final int PORT_8 = 8;

    /** The port for test server 9. */
    private final int PORT_9 = 9;

    /** The endpoint name for test server 0. */
    public final NodeLocator ENDPOINT_0 = buildTestEndpoint(PORT_0);

    /** The endpoint name for test server 1. */
    public final NodeLocator ENDPOINT_1 = buildTestEndpoint(PORT_1);

    /** The endpoint name for test server 2. */
    public final NodeLocator ENDPOINT_2 = buildTestEndpoint(PORT_2);

    /** The endpoint name for test server 3. */
    public final NodeLocator ENDPOINT_3 = buildTestEndpoint(PORT_3);

    /** The endpoint name for test server 4. */
    public final NodeLocator ENDPOINT_4 = buildTestEndpoint(PORT_4);

    /** The endpoint name for test server 5. */
    public final NodeLocator ENDPOINT_5 = buildTestEndpoint(PORT_5);

    /** The endpoint name for test server 6. */
    public final NodeLocator ENDPOINT_6 = buildTestEndpoint(PORT_6);

    /** The endpoint name for test server 7. */
    public final NodeLocator ENDPOINT_7 = buildTestEndpoint(PORT_7);

    /** The endpoint name for test server 8. */
    public final NodeLocator ENDPOINT_8 = buildTestEndpoint(PORT_8);

    /** The endpoint name for test server 9. */
    public final NodeLocator ENDPOINT_9 = buildTestEndpoint(PORT_9);

    public static NodeLocator buildTestEndpoint(int port) {
        return NodeLocator.builder().host(HOST_NAME).port(port).build();
    }

    public static boolean isNotTestEndpoint(NodeLocator endpoint) {
        return !CorfuTestServers.HOST_NAME.equals(endpoint.getHost());
    }

    public static void checkTestEndpoint(NodeLocator endpoint){
        if (isNotTestEndpoint(endpoint)) {
            throw new RuntimeException("Unsupported endpoint in test: " + endpoint.toEndpointUrl());
        }
    }
}
