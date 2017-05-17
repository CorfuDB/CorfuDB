package org.corfudb.runtime;

import com.codahale.metrics.MetricRegistry;
import lombok.experimental.Accessors;
import org.corfudb.runtime.clients.IClientRouter;
import org.corfudb.runtime.clients.LayoutClient;
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.runtime.clients.ManagementClient;
import org.corfudb.runtime.clients.NettyClientRouter;
import org.corfudb.runtime.clients.SequencerClient;
import org.corfudb.runtime.view.AddressSpaceView;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.LayoutView;
import org.corfudb.runtime.view.ObjectsView;
import org.corfudb.runtime.view.SequencerView;
import org.corfudb.runtime.view.StreamsView;
import org.corfudb.util.GitRepositoryState;
import org.corfudb.util.MetricsUtils;
import org.corfudb.util.Version;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Created by mwei on 12/9/15.
 */
@Accessors(chain = true)
public class CorfuRuntime {

    private static final Logger log = org.slf4j.LoggerFactory.getLogger(CorfuRuntime.class);

    public static String getMpASV() {
        return CorfuRuntime.mpASV;
    }

    public static String getMpLUC() {
        return CorfuRuntime.mpLUC;
    }

    public static String getMpCR() {
        return CorfuRuntime.mpCR;
    }

    public static String getMpObj() {
        return CorfuRuntime.mpObj;
    }

    public static MetricRegistry getMetrics() {
        return CorfuRuntime.metrics;
    }

    public CorfuRuntimeParameters getParameters() {
        return this.parameters;
    }

    public LayoutView getLayoutView() {
        return this.layoutView;
    }

    public SequencerView getSequencerView() {
        return this.sequencerView;
    }

    public AddressSpaceView getAddressSpaceView() {
        return this.addressSpaceView;
    }

    public StreamsView getStreamsView() {
        return this.streamsView;
    }

    public ObjectsView getObjectsView() {
        return this.objectsView;
    }

    public boolean isCacheDisabled() {
        return this.cacheDisabled;
    }

    public long getMaxCacheSize() {
        return this.maxCacheSize;
    }

    public boolean isBackpointersDisabled() {
        return this.backpointersDisabled;
    }

    public boolean isHoleFillingDisabled() {
        return this.holeFillingDisabled;
    }

    public boolean isShutdown() {
        return this.isShutdown;
    }

    public Function<String, IClientRouter> getGetRouterFunction() {
        return this.getRouterFunction;
    }

    public CorfuRuntime setMaxCacheSize(long maxCacheSize) {
        this.maxCacheSize = maxCacheSize;
        return this;
    }

    public CorfuRuntime setHoleFillingDisabled(boolean holeFillingDisabled) {
        this.holeFillingDisabled = holeFillingDisabled;
        return this;
    }

    public CorfuRuntime setGetRouterFunction(Function<String, IClientRouter> getRouterFunction) {
        this.getRouterFunction = getRouterFunction;
        return this;
    }

    public static class CorfuRuntimeParameters {

        /** True, if undo logging is disabled. */
        boolean undoDisabled = false;

        /** True, if optimistic undo logging is disabled. */
        boolean optimisticUndoDisabled = false;

        /** Number of times to attempt to read before hole filling. */
        int holeFillRetry = 10;

        public CorfuRuntimeParameters() {
        }

        public boolean isUndoDisabled() {
            return this.undoDisabled;
        }

        public boolean isOptimisticUndoDisabled() {
            return this.optimisticUndoDisabled;
        }

        public int getHoleFillRetry() {
            return this.holeFillRetry;
        }

        public CorfuRuntimeParameters setUndoDisabled(boolean undoDisabled) {
            this.undoDisabled = undoDisabled;
            return this;
        }

        public CorfuRuntimeParameters setOptimisticUndoDisabled(boolean optimisticUndoDisabled) {
            this.optimisticUndoDisabled = optimisticUndoDisabled;
            return this;
        }

        public CorfuRuntimeParameters setHoleFillRetry(int holeFillRetry) {
            this.holeFillRetry = holeFillRetry;
            return this;
        }

        public boolean equals(Object o) {
            if (o == this) return true;
            if (!(o instanceof CorfuRuntimeParameters)) return false;
            final CorfuRuntimeParameters other = (CorfuRuntimeParameters) o;
            if (!other.canEqual((Object) this)) return false;
            if (this.isUndoDisabled() != other.isUndoDisabled()) return false;
            if (this.isOptimisticUndoDisabled() != other.isOptimisticUndoDisabled()) return false;
            if (this.getHoleFillRetry() != other.getHoleFillRetry()) return false;
            return true;
        }

        public int hashCode() {
            final int PRIME = 59;
            int result = 1;
            result = result * PRIME + (this.isUndoDisabled() ? 79 : 97);
            result = result * PRIME + (this.isOptimisticUndoDisabled() ? 79 : 97);
            result = result * PRIME + this.getHoleFillRetry();
            return result;
        }

        protected boolean canEqual(Object other) {
            return other instanceof CorfuRuntimeParameters;
        }

        public String toString() {
            return "org.corfudb.runtime.CorfuRuntime.CorfuRuntimeParameters(undoDisabled=" + this.isUndoDisabled() + ", optimisticUndoDisabled=" + this.isOptimisticUndoDisabled() + ", holeFillRetry=" + this.getHoleFillRetry() + ")";
        }
    }

    private final CorfuRuntimeParameters parameters = new CorfuRuntimeParameters();
    /**
     * A view of the layout service in the Corfu server instance.
     */
    private final LayoutView layoutView = new LayoutView(this);
    /**
     * A view of the sequencer server in the Corfu server instance.
     */
    private final SequencerView sequencerView = new SequencerView(this);
    /**
     * A view of the address space in the Corfu server instance.
     */
    private final AddressSpaceView addressSpaceView = new AddressSpaceView(this);
    /**
     * A view of streamsView in the Corfu server instance.
     */
    private final StreamsView streamsView = new StreamsView(this);

    //region Address Space Options
    /**
     * Views of objects in the Corfu server instance.
     */
    private final ObjectsView objectsView = new ObjectsView(this);
    /**
     * A list of known layout servers.
     */
    private List<String> layoutServers;

    //endregion Address Space Options
    /**
     * A map of routers, representing nodes.
     */
    public Map<String, IClientRouter> nodeRouters;
    /**
     * A completable future containing a layout, when completed.
     */
    public volatile CompletableFuture<Layout> layout;
    /**
     * The rate in seconds to retry accessing a layout, in case of a failure.
     */
    public int retryRate;
    /**
     * Whether or not to disable the cache.
     */
    public boolean cacheDisabled = false;
    /**
     * The maximum size of the cache, in bytes.
     */
    public long maxCacheSize = 4_000_000_000L;

    /**
     * Whether or not to disable backpointers.
     */
    public boolean backpointersDisabled = false;

    /**
     * If hole filling is disabled.
     */
    public boolean holeFillingDisabled = false;

    /**
     * Notifies that the runtime is no longer used
     * and async retries to fetch the layout can be stopped.
     */
    private volatile boolean isShutdown = false;

    private boolean tlsEnabled = false;
    private String keyStore;
    private String ksPasswordFile;
    private String trustStore;
    private String tsPasswordFile;

    private boolean saslPlainTextEnabled = false;
    private String usernameFile;
    private String passwordFile;

    /**
     * Metrics: meter (counter), histogram
     */
    static private final String mp = "corfu.runtime.";
    static private final String mpASV = mp + "as-view.";
    static private final String mpLUC = mp + "log-unit-client.";
    static private final String mpCR = mp + "client-router.";
    static private final String mpObj = mp + "object.";
    static public final MetricRegistry metrics = new MetricRegistry();

    /**
     * When set, overrides the default getRouterFunction. Used by the testing
     * framework to ensure the default routers used are for testing.
     */
    public static BiFunction<CorfuRuntime, String, IClientRouter> overrideGetRouterFunction = null;

    /**
     * A function to handle getting routers. Used by test framework to inject
     * a test router. Can also be used to provide alternative logic for obtaining
     * a router.
     */
    public Function<String, IClientRouter> getRouterFunction = overrideGetRouterFunction != null ?
            (address) -> overrideGetRouterFunction.apply(this, address) : (address) -> {

        // Return an existing router if we already have one.
        if (nodeRouters.containsKey(address)) {
            return nodeRouters.get(address);
        }
        // Parse the string in host:port format.
        String host = address.split(":")[0];
        Integer port = Integer.parseInt(address.split(":")[1]);
        // Generate a new router, start it and add it to the table.
        NettyClientRouter router = new NettyClientRouter(host, port,
            tlsEnabled, keyStore, ksPasswordFile, trustStore, tsPasswordFile,
            saslPlainTextEnabled, usernameFile, passwordFile);
        log.debug("Connecting to new router {}:{}", host, port);
        try {
            router.addClient(new LayoutClient())
                    .addClient(new SequencerClient())
                    .addClient(new LogUnitClient())
                    .addClient(new ManagementClient())
                    .start();
            nodeRouters.put(address, router);
        } catch (Exception e) {
            log.warn("Error connecting to router", e);
        }
        return router;
    };

    public CorfuRuntime() {
        layoutServers = new ArrayList<>();
        nodeRouters = new ConcurrentHashMap<>();
        retryRate = 5;
        synchronized (metrics) {
            if (metrics.getNames().isEmpty()) {
                MetricsUtils.addJVMMetrics(metrics, mp);
                MetricsUtils.metricsReportingSetup(metrics);
            }
        }
        log.debug("Corfu runtime version {} initialized.", getVersionString());
    }

    /**
     * Parse a configuration string and get a CorfuRuntime.
     *
     * @param configurationString The configuration string to parse.
     */
    public CorfuRuntime(String configurationString) {
        this();
        this.parseConfigurationString(configurationString);
    }

    public CorfuRuntime enableTls(String keyStore, String ksPasswordFile, String trustStore,
        String tsPasswordFile) {
        this.keyStore = keyStore;
        this.ksPasswordFile = ksPasswordFile;
        this.trustStore = trustStore;
        this.tsPasswordFile = tsPasswordFile;
        this.tlsEnabled = true;
        return this;
    }

    public CorfuRuntime enableSaslPlainText(String usernameFile, String passwordFile) {
        this.usernameFile = usernameFile;
        this.passwordFile = passwordFile;
        this.saslPlainTextEnabled = true;
        return this;
    }
    /**
     * Shuts down the CorfuRuntime.
     * Stops async tasks from fetching the layout.
     * Cannot reuse the runtime once shutdown is called.
     */
    public void shutdown() {

        // Stopping async task from fetching layout.
        isShutdown = true;
        if (layout != null) {
            try {
                layout.cancel(true);
            } catch (Exception e) {
                log.error("Runtime shutting down. Exception in terminating fetchLayout: {}", e);
            }
        }
        stop(true);
    }

    /**
     * Stop all routers associated with this runtime & disconnect them.
     */
    public void stop() {
        stop(false);
    }

    public void stop(boolean shutdown) {
        for (IClientRouter r: nodeRouters.values()) {
            r.stop(shutdown);
        }
        if (!shutdown) {
            // N.B. An icky side-effect of this clobbering is leaking
            // Pthreads, namely the Netty client-side worker threads.
            nodeRouters = new ConcurrentHashMap<>();
        }
    }

    /**
     * Get a UUID for a named stream.
     *
     * @param string The name of the stream.
     * @return The ID of the stream.
     */
    public static UUID getStreamID(String string) {
        return UUID.nameUUIDFromBytes(string.getBytes());
    }

    public static String getVersionString() {
        if (Version.getVersionString().contains("SNAPSHOT") || Version.getVersionString().contains("source")) {
            return Version.getVersionString() + "(" + GitRepositoryState.getRepositoryState().commitIdAbbrev + ")";
        }
        return Version.getVersionString();
    }

    /**
     * Whether or not to disable backpointers
     *
     * @param disable True, if the cache should be disabled, false otherwise.
     * @return A CorfuRuntime to support chaining.
     */
    public CorfuRuntime setBackpointersDisabled(boolean disable) {
        this.backpointersDisabled = disable;
        return this;
    }

    /**
     * Whether or not to disable the cache
     *
     * @param disable True, if the cache should be disabled, false otherwise.
     * @return A CorfuRuntime to support chaining.
     */
    public CorfuRuntime setCacheDisabled(boolean disable) {
        this.cacheDisabled = disable;
        return this;
    }

    /**
     * If enabled, successful transactions will be written to a special transaction stream (i.e. TRANSACTION_STREAM_ID)
     * @param enable
     * @return
     */
    public CorfuRuntime setTransactionLogging(boolean enable) {
        this.getObjectsView().setTransactionLogging(enable);
        return this;
    }

    /**
     * Parse a configuration string and get a CorfuRuntime.
     *
     * @param configurationString The configuration string to parse.
     * @return A CorfuRuntime Configured based on the configuration string.
     */
    public CorfuRuntime parseConfigurationString(String configurationString) {
        // Parse comma sep. list.
        layoutServers = Pattern.compile(",")
                .splitAsStream(configurationString)
                .map(String::trim)
                .collect(Collectors.toList());
        return this;
    }

    /**
     * Add a layout server to the list of servers known by the CorfuRuntime.
     *
     * @param layoutServer A layout server to use.
     * @return A CorfuRuntime, to support the builder pattern.
     */
    public CorfuRuntime addLayoutServer(String layoutServer) {
        layoutServers.add(layoutServer);
        return this;
    }

    /**
     * Get a router, given the address.
     *
     * @param address The address of the router to get.
     * @return The router.
     */
    public IClientRouter getRouter(String address) {
        return getRouterFunction.apply(address);
    }

    /**
     * Invalidate the current layout.
     * If the layout has been previously invalidated and a new layout has not yet been retrieved,
     * this function does nothing.
     */
    public synchronized void invalidateLayout() {
        // Is there a pending request to retrieve the layout?
        if (!layout.isDone()) {
            // Don't create a new request for a layout if there is one pending.
            return;
        }
        layout = fetchLayout();
    }


    /**
     * Return a completable future which is guaranteed to contain a layout.
     * This future will continue retrying until it gets a layout. If you need this completable future to fail,
     * you should chain it with a timeout.
     *
     * @return A completable future containing a layout.
     */
    private CompletableFuture<Layout> fetchLayout() {
        return CompletableFuture.<Layout>supplyAsync(() -> {

            while (true) {
                List<String> layoutServersCopy =  layoutServers.stream().collect(Collectors.toList());
                Collections.shuffle(layoutServersCopy);
                // Iterate through the layout servers, attempting to connect to one
                for (String s : layoutServersCopy) {
                    log.debug("Trying connection to layout server {}", s);
                    try {
                        IClientRouter router = getRouter(s);
                        // Try to get a layout.
                        CompletableFuture<Layout> layoutFuture = router.getClient(LayoutClient.class).getLayout();
                        // Wait for layout
                        Layout l = layoutFuture.get();
                        l.setRuntime(this);
                        // this.layout should only be assigned to the new layout future once it has been
                        // completely constructed and initialized. For example, assigning this.layout = l
                        // before setting the layout's runtime can result in other threads trying to access a layout
                        // with  a null runtime.
                        //FIXME Synchronization START
                        // We are updating multiple variables and we need the update to be synchronized across all variables.
                        // Since the variable layoutServers is used only locally within the class it is acceptable
                        // (at least the code on 10/13/2016 does not have issues)
                        // but setEpoch of routers needs to be synchronized as those variables are not local.
                        l.getAllServers().stream().map(getRouterFunction).forEach(x -> x.setEpoch(l.getEpoch()));
                        layoutServers = l.getLayoutServers();
                        layout = layoutFuture;
                        //FIXME Synchronization END

                        log.debug("Layout server {} responded with layout {}", s, l);
                        return l;
                    } catch (Exception e) {
                        log.warn("Tried to get layout from {} but failed with exception:", s, e);
                    }
                }
                log.warn("Couldn't connect to any layout servers, retrying in {}s.", retryRate);
                try {
                    Thread.sleep(retryRate * 1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                if (isShutdown) {
                    return null;
                }
            }
        });
    }

    /**
     * Connect to the Corfu server instance.
     * When this function returns, the Corfu server is ready to be accessed.
     */
    public synchronized CorfuRuntime connect() {
        if (layout == null) {
            log.info("Connecting to Corfu server instance, layout servers={}", layoutServers);
            // Fetch the current layout and save the future.
            layout = fetchLayout();
            try {
                layout.get();
            } catch (Exception e) {
                // A serious error occurred trying to connect to the Corfu instance.
                log.error("Fatal error connecting to Corfu server instance.", e);
                throw new RuntimeException(e);
            }
        }
        return this;
    }
}
