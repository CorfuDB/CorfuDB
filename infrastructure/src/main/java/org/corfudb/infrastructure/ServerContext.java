package org.corfudb.infrastructure;

import lombok.Getter;
import lombok.Setter;
import org.corfudb.runtime.view.Layout;

import java.time.Duration;
import java.util.Map;

/**
 * Server Context:
 * <ul>
 * <li>Contains the common node level {@link DataStore}</li>
 * <li>Responsible for Server level EPOCH </li>
 * <li>Should contain common services/utilities that the different Servers in a node require.</li>
 * </ul>
 *
 * Note:
 * It is created in {@link CorfuServer} and then
 * passed to all the servers including {@link NettyServerRouter}.
 *
 * Created by mdhawan on 8/5/16.
 */
public class ServerContext {
    private static final String PREFIX_EPOCH = "SERVER_EPOCH";
    private static final String KEY_EPOCH = "CURRENT";

    /**
     * magic non-address value, used in parameters to indicate no valid log address is provided
     */
    public static final long NON_LOG_ADDR_MAGIC = -1L;

    /**
     * various duration constants
     */
    public static final Duration SMALL_INTERVAL = Duration.ofMillis(60_000);
    public static final Duration SHUTDOWN_TIMER = Duration.ofSeconds(5);


    @Getter
    private final Map<String, Object> serverConfig;

    @Getter
    private final DataStore dataStore;

    @Getter
    private IServerRouter serverRouter;

    @Getter
    @Setter
    private IFailureDetectorPolicy failureDetectorPolicy;

    @Getter
    @Setter
    private IFailureHandlerPolicy failureHandlerPolicy;

    public ServerContext(Map<String, Object> serverConfig, IServerRouter serverRouter) {
        this.serverConfig = serverConfig;
        this.dataStore = new DataStore(serverConfig);
        this.serverRouter = serverRouter;
        this.failureDetectorPolicy = new PeriodicPollPolicy();
        this.failureHandlerPolicy = new PurgeFailurePolicy();
    }

    /**
     * The epoch of this router. This is managed by the base server implementation.
     */
    public synchronized long getServerEpoch() {
        Long epoch = dataStore.get(Long.class, PREFIX_EPOCH, KEY_EPOCH);
        return epoch == null ? 0 : epoch;
    }

    public synchronized void setServerEpoch(long serverEpoch) {
        dataStore.put(Long.class, PREFIX_EPOCH, KEY_EPOCH, serverEpoch);
        // Set the epoch in the router as well.
        //TODO need to figure out if we can remove this redundancy
        serverRouter.setServerEpoch(serverEpoch);
    }
}
