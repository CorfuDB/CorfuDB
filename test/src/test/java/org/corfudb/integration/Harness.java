package org.corfudb.integration;

import static org.corfudb.AbstractCorfuTest.PARAMETERS;
import static org.corfudb.integration.AbstractIT.PROPERTIES;

import lombok.Builder;
import lombok.Getter;
import org.corfudb.integration.cluster.Harness.Action;
import org.corfudb.integration.cluster.Harness.Node;
import org.corfudb.runtime.BootstrapUtil;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Layout;
import org.corfudb.test.CorfuServerRunner;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * Utilities to form and manipulate a corfu cluster. This class provides a convenience instance builder
 * to create Harness based on the properties in CorfuDB.properties. In addition to that a Builder is
 * provides in order to create instances of Harness with specific properties.
 *
 * Created by maithem on 7/18/18.
 */

@Getter
@Builder
public class Harness {

    private final int corfuPort;
    private final String corfuHost;
    private final String corfuEndpoint;

    private final boolean tlsEnabled;
    private final String runtimePathToKeyStore;
    private final String runtimePathToKeyStorePassword;
    private final String runtimePathToTrustStore;
    private final String runtimePathToTrustStorePassword;
    private final String serverPathToKeyStore;
    private final String serverPathToKeyStorePassword;
    private final String serverPathToTrustStore;
    private final String serverPathToTrustStorePassword;
    private final int connectionTimeout;

    /**
     * This is a helper instance builder that creates an instance of {@link Harness} using the
     * properties from the values provided in CorfuDB.properties. Note that it will throw
     * {@link NumberFormatException} if the numerical properties provided in CorfuDB.properties are
     * not well-formed. If the CorfuDB.properties has invalid or non existent values for the keystore or
     *      * truststore, it will lead to throwing {@link IllegalArgumentException}.
     *
     * @return returns an instance of {@link Harness} created using properties in CorfuDB.properties
     */
    public static Harness getDefaultHarness() {
        final String corfuHost = PROPERTIES.getProperty("corfuHost");
        final int corfuPort = Integer.parseInt(PROPERTIES.getProperty("corfuPort"));
        final Boolean tlsEnabled = Boolean.valueOf(PROPERTIES.getProperty("tlsEnabledHarness"));

        HarnessBuilder harnessBuilder = new HarnessBuilder()
                .corfuHost(corfuHost)
                .corfuPort(corfuPort)
                .corfuEndpoint(String.format("%s:%d", corfuHost, corfuPort))
                .tlsEnabled(tlsEnabled)
                .connectionTimeout(Integer.parseInt(PROPERTIES.getProperty("connectionTimeout")));
        if (tlsEnabled) {
            harnessBuilder
                    .serverPathToKeyStore(
                            getPropertyAbsolutePath("serverPathToKeyStore"))
                    .serverPathToKeyStorePassword(
                            getPropertyAbsolutePath("serverPathToKeyStorePassword"))
                    .serverPathToTrustStore(
                            getPropertyAbsolutePath("serverPathToTrustStore"))
                    .serverPathToTrustStorePassword(
                            getPropertyAbsolutePath("serverPathToTrustStorePassword"))
                    .runtimePathToKeyStore(
                            getPropertyAbsolutePath("runtimePathToKeyStore"))
                    .runtimePathToKeyStorePassword(
                            getPropertyAbsolutePath("runtimePathToKeyStorePassword"))
                    .runtimePathToTrustStore(
                            getPropertyAbsolutePath("runtimePathToTrustStore"))
                    .runtimePathToTrustStorePassword(
                            getPropertyAbsolutePath("runtimePathToTrustStorePassword"));
        }
        return harnessBuilder.build();
    }

    /**
     * Take the property string provided int CorfuDB.properties and return the absolute path string. Throws
     * {@link IllegalArgumentException} if the the provided path is non existent or invalid.
     *
     * @param pathProperty a property in CorfuDB.properties file pointing to a file whose absolute path
     *                     required to be loaded.
     * @return absolute path provided in CorfuDB.properties for the pathProperty
     */
    private static String getPropertyAbsolutePath(String pathProperty) {
        if ((PROPERTIES.getProperty(pathProperty)) == null ||
                Files.notExists(Paths.get(PROPERTIES.getProperty(pathProperty)).toAbsolutePath())) {
            throw new IllegalArgumentException(
                    String.format("CorfuDB.properties contains invalid or non existent value for :%s",
                            pathProperty));
        }

        return Paths.get(PROPERTIES.getProperty(pathProperty)).toAbsolutePath().toString();
    }

    private String getAddressForNode(int port) {
        return corfuHost + ":" + port;
    }

    /**
     * Creates an n-node cluster layout.
     *
     * @param nodeCount number of nodes in the cluster
     * @return a layout for the cluster
     */
    private Layout getLayoutForNodes(int nodeCount) {
        List<String> layoutServers = new ArrayList<>();
        List<String> sequencer = new ArrayList<>();
        List<Layout.LayoutSegment> segments = new ArrayList<>();
        UUID clusterId = UUID.randomUUID();
        List<String> stripServers = new ArrayList<>();

        long epoch = 0;

        for (int x = 0; x < nodeCount; x++) {
            int port = corfuPort + x;
            layoutServers.add(getAddressForNode(port));
            sequencer.add(getAddressForNode(port));
            stripServers.add(getAddressForNode(port));
        }

        Layout.LayoutSegment segment = new Layout.LayoutSegment(Layout.ReplicationMode.CHAIN_REPLICATION, 0L, -1L,
                Collections.singletonList(new Layout.LayoutStripe(stripServers)));
        segments.add(segment);
        return new Layout(layoutServers, sequencer, segments, epoch, clusterId);
    }

    String getClusterConnectionString(int n) {
        StringBuilder conn = new StringBuilder();

        for (int i = 0; i < n; i++) {
            int port = corfuPort + i;
            conn.append(corfuHost)
                .append(":")
                .append(port)
                .append(",");
        }

        // return final connection string without tailing comma
        return conn.length() > 0 ? conn.toString().substring(0, conn.length() - 1) : "" ;
    }

    /**
     * Run a action(n) on nodes
     *
     * @param actions node actions
     * @throws Exception
     */
    public static void run(Action... actions) throws Exception {
        for (Action action : actions) {
            action.run();
        }
    }

    /**
     * Deploy an n-node cluster
     *
     * @param n number of nodes in a cluster
     * @return a list of provisioned nodes in the cluster
     * @throws IOException
     */
    public List<Node> deployCluster(int n) throws IOException {
        List<Node> nodes = new ArrayList<>(n);
        String conn = getClusterConnectionString(n);
        for (int i = 0; i < n; i++) {
            int port = corfuPort + i;
            Process proc = runServerOnPort(port);
            Node node = new Node(
                    getAddressForNode(port),
                    conn,
                    CorfuServerRunner.getCorfuServerLogPath(corfuHost, port)
            );
            nodes.add(node);
        }

        final Layout layout = getLayoutForNodes(n);
        final int retries = 3;
        CorfuRuntime.CorfuRuntimeParameters runtimeParameters = createRuntimeParameters();
        BootstrapUtil.bootstrap(layout, runtimeParameters, retries, PARAMETERS.TIMEOUT_LONG);
        return nodes;
    }

    /**
     * This method uses the properties initialized by the Harness builder.
     * and creates an {@link org.corfudb.runtime.CorfuRuntime.CorfuRuntimeParameters}.
     * If TLS is enabled, the instance captures, keystore, truststore the corresponding properties.
     * Otherwise, it will create and return the parameters without setting TLS properties.
     *
     * @return an instance of {@link org.corfudb.runtime.CorfuRuntime.CorfuRuntimeParameters}
     */
    private CorfuRuntime.CorfuRuntimeParameters createRuntimeParameters() {
        final CorfuRuntime.CorfuRuntimeParameters.CorfuRuntimeParametersBuilder runtimeParametersBuilder =
                CorfuRuntime.CorfuRuntimeParameters
                .builder()
                .connectionTimeout(Duration.ofMillis(connectionTimeout));

        return tlsEnabled ?
                runtimeParametersBuilder
                        .tlsEnabled(tlsEnabled)
                        .keyStore(serverPathToKeyStore)
                        .ksPasswordFile(serverPathToKeyStorePassword)
                        .trustStore(serverPathToTrustStore)
                        .tsPasswordFile(serverPathToTrustStorePassword)
                        .build() :
                runtimeParametersBuilder
                        .build();
    }

    /**
     * A helper method that runs a Corfu server on the provided port and returns a
     * corresponding process. It uses the properties set in CorfuDB.properties
     * for spawning the server. Note that if the properties indicate TLS being enabled,
     * the Corfu server will be created using the keystore and truststore provided. But
     * if TLS is disabled the server being run through {@link AbstractIT.CorfuServerRunner}
     * will not use TLS.
     *
     * @param port A port on which the Corfu server will be run
     *
     * @return the server process
     * @throws IOException
     */
    private Process runServerOnPort(int port) throws IOException {
        return new CorfuServerRunner()
                .setHost(corfuHost)
                .setPort(port)
                .setTlsEnabled(tlsEnabled)
                .setKeyStore(serverPathToKeyStore)
                .setKeyStorePassword(serverPathToKeyStorePassword)
                .setTrustStore(serverPathToTrustStore)
                .setTrustStorePassword(serverPathToTrustStorePassword)
                .setLogPath(CorfuServerRunner.getCorfuServerLogPath(corfuHost, port))
                .setSingle(false)
                .runServer();
    }

    /**
     * Deploys an unbootstrapped node.
     *
     * @param port Port to bind node to.
     * @return Node instance.
     * @throws IOException
     */
    public Node deployUnbootstrappedNode(int port) throws IOException {

        Process proc = runServerOnPort(port);
        return new Node(getAddressForNode(port), CorfuServerRunner.getCorfuServerLogPath(corfuHost, port));
    }

    /**
     * This method creates a Corfu Runtime for a particular node. The created runtime
     * uses the corresponding properties of the Harness used for creating the cluster
     *
     * @param node
     *
     * @return an instance of CorfuRuntime configured with TLS parameters provided in CorfuDB.properties
     */
    public CorfuRuntime createRuntimeForNode(Node node) {
        final CorfuRuntime runtime = new CorfuRuntime(node.getClusterAddress());

        if (tlsEnabled) {
            runtime.enableTls(
                    runtimePathToKeyStore,
                    runtimePathToKeyStorePassword,
                    runtimePathToTrustStore,
                    runtimePathToTrustStorePassword);
        }
        return runtime.connect();
    }
}
