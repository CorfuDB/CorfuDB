package org.corfudb.infrastructure;

import com.codahale.metrics.MetricRegistry;

import java.time.Duration;
import java.util.Map;

import java.util.UUID;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.Setter;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.management.IFailureDetectorPolicy;
import org.corfudb.infrastructure.management.PeriodicPollPolicy;
import org.corfudb.runtime.view.ConservativeFailureHandlerPolicy;
import org.corfudb.runtime.view.IFailureHandlerPolicy;
import org.corfudb.util.MetricsUtils;
import org.corfudb.util.UuidUtils;

import static org.corfudb.util.MetricsUtils.isMetricsReportingSetUp;

/**
 * Server Context:
 * <ul>
 * <li>Contains the common node level {@link DataStore}</li>
 * <li>Responsible for Server level EPOCH </li>
 * <li>Should contain common services/utilities that the different Servers in a node require.</li>
 * </ul>
 *
 * <p>Note:
 * It is created in {@link CorfuServer} and then
 * passed to all the servers including {@link NettyServerRouter}.
 *
 * <p>Created by mdhawan on 8/5/16.
 */
@Slf4j
public class ServerContext {
    private static final String PREFIX_EPOCH = "SERVER_EPOCH";
    private static final String KEY_EPOCH = "CURRENT";
    private static final String PREFIX_TAIL_SEGMENT = "TAIL_SEGMENT";
    private static final String KEY_TAIL_SEGMENT = "CURRENT";
    private static final String PREFIX_STARTING_ADDRESS = "STARTING_ADDRESS";
    private static final String KEY_STARTING_ADDRESS = "CURRENT";

    /** The node Id, stored as a base64 string. */
    public static final String NODE_ID = "NODE_ID";

    /**
     * various duration constants.
     */
    public static final Duration SMALL_INTERVAL = Duration.ofMillis(60_000);
    public static final Duration SHUTDOWN_TIMER = Duration.ofSeconds(5);


    @Getter
    private final Map<String, Object> serverConfig;

    @Getter
    private final DataStore dataStore;

    @Getter
    @Setter
    private IServerRouter serverRouter;

    @Getter
    @Setter
    private IFailureDetectorPolicy failureDetectorPolicy;

    @Getter
    @Setter
    private IFailureHandlerPolicy failureHandlerPolicy;

    @Getter
    public static final MetricRegistry metrics = new MetricRegistry();

    /**
     * Returns a new ServerContext.
     * @param serverConfig map of configuration strings to objects
     */
    public ServerContext(Map<String, Object> serverConfig) {
        this.serverConfig = serverConfig;
        this.dataStore = new DataStore(serverConfig);
        generateNodeId();
        this.failureDetectorPolicy = new PeriodicPollPolicy();
        this.failureHandlerPolicy = new ConservativeFailureHandlerPolicy();

        // Metrics setup & reporting configuration
        String mp = "corfu.server.";
        synchronized (metrics) {
            if (!isMetricsReportingSetUp(metrics)) {
//                addJvmMetrics(metrics, mp);
//                MetricsUtils.addCacheGauges(metrics, mp + "datastore.cache.", dataStore.getCache());
                MetricsUtils.metricsReportingSetup(metrics);
            }
        }
    }


    /** Generate a Node Id if not present.
     *
     */
    private void generateNodeId() {
        String currentId = getDataStore().get(String.class, "", ServerContext.NODE_ID);
        if (currentId == null) {
            String idString = UuidUtils.asBase64(UUID.randomUUID());
            log.info("No Node Id, setting to new Id={}", idString);
            getDataStore().put(String.class, "", ServerContext.NODE_ID, idString);
        } else {
            log.info("Node Id = {}", currentId);
        }
    }

    /** Get the node id as an UUID.
     *
     * @return  A UUID for this node.
     */
    public UUID getNodeId() {
        return UuidUtils.fromBase64(getNodeIdBase64());
    }

    /** Get the node id as a base64 string.
     *
     * @return A node ID for this node, as a base64 string.
     */
    public String getNodeIdBase64() {
        return getDataStore().get(String.class, "", ServerContext.NODE_ID);
    }

    /** Get a field from the server configuration map.
     *
     * @param type          The type of the field.
     * @param optionName    The name of the option to retrieve.
     * @param <T>           The type of the field to return.
     * @return              The field with the give option name.
     */
    @SuppressWarnings("unchecked")
    public <T> T getServerConfig(Class<T> type, String optionName) {
        return (T) getServerConfig().get(optionName);
    }

    /**
     * The epoch of this router. This is managed by the base server implementation.
     */
    public long getServerEpoch() {
        Long epoch = dataStore.get(Long.class, PREFIX_EPOCH, KEY_EPOCH);
        return epoch == null ? 0 : epoch;
    }

    /**
     * Set the serverRouter epoch.
     * @param serverEpoch the epoch to set
     */
    public void setServerEpoch(long serverEpoch, IServerRouter r) {
        dataStore.put(Long.class, PREFIX_EPOCH, KEY_EPOCH, serverEpoch);
        r.setServerEpoch(serverEpoch);
    }

    public long getTailSegment() {
        Long tailSegment = dataStore.get(Long.class, PREFIX_TAIL_SEGMENT, KEY_TAIL_SEGMENT);
        return tailSegment == null ? 0 : tailSegment;
    }

    public void setTailSegment(long tailSegment) {
        dataStore.put(Long.class, PREFIX_TAIL_SEGMENT, KEY_TAIL_SEGMENT, tailSegment);
    }

    /**
     * Returns the dataStore starting address.
     * @return the starting address
     */
    public long getStartingAddress() {
        Long startingAddress = dataStore.get(Long.class, PREFIX_STARTING_ADDRESS,
                KEY_STARTING_ADDRESS);
        return startingAddress == null ? 0 : startingAddress;
    }

    public void setStartingAddress(long startingAddress) {
        dataStore.put(Long.class, PREFIX_STARTING_ADDRESS, KEY_STARTING_ADDRESS, startingAddress);
    }
}
