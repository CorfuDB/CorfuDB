package org.corfudb.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.MsgHandlingFilter;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuRuntime.CorfuRuntimeParameters;
import org.corfudb.test.CorfuServerRunner;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

/**
 * This testsuite exercises examples of using Corfu message filters which are applied by
 * Netty Client Router's filter handler on inbound corfu messages.
 *
 * Created by Sam Behnam on 5/30/18.
 */
public class CorfuMsgFilterIT extends AbstractIT {

    private static String corfuSingleNodeHost;
    private static int corfuSingleNodePort;
    private static String singleNodeEndpoint;

    // Helper method to run a single persistent server for provided address and port
    private Process runSinglePersistentServer(String address, int port) throws IOException {
        return new CorfuServerRunner()
                .setHost(address)
                .setPort(port)
                .setLogPath(CorfuServerRunner.getCorfuServerLogPath(address, port))
                .setSingle(true)
                .runServer();
    }

    @Before
    public  void loadProperties() {
        corfuSingleNodeHost = PROPERTIES.getProperty("corfuSingleNodeHost");
        corfuSingleNodePort = Integer.valueOf(PROPERTIES.getProperty("corfuSingleNodePort"));
        singleNodeEndpoint = String.format("%s:%d",
                corfuSingleNodeHost, corfuSingleNodePort);
    }

    /**
     * This test enables filtering of PONG {@link CorfuMsgType} and shows that such message dropping
     * won't affect the initialization of a {@link CorfuRuntime}.
     *
     * @throws Exception
     */
    @Test
    public void test1NodeSimulationDropPong() throws Exception {
        // Run a single persistent server
        Process corfuServer = runSinglePersistentServer(corfuSingleNodeHost, corfuSingleNodePort);

        // Create a collection of filters with a filter dropping all PONG messages
        List<MsgHandlingFilter> handlingFilters = new ArrayList<>();
        final Function<CorfuMsg, CorfuMsg> dropPong = a -> (a.getMsgType() == CorfuMsgType.PONG ? null : a);

        MsgHandlingFilter filter = new MsgHandlingFilter(a -> true, dropPong);
        handlingFilters.add(filter);

        // Build parameters for CorfuRuntime
        final CorfuRuntimeParameters.CorfuRuntimeParametersBuilder parametersBuilder =
                CorfuRuntime.CorfuRuntimeParameters.builder();
        parametersBuilder.nettyClientInboundMsgFilters(handlingFilters);
        final CorfuRuntimeParameters corfuRuntimeParameters = parametersBuilder.build();

        // Initialize and connect to a corfuRuntime
        runtime = CorfuRuntime
                .fromParameters(corfuRuntimeParameters)
                .parseConfigurationString(singleNodeEndpoint)
                .connect();

        // Verify
        assertThat(runtime.getLayoutView().getLayout().getEpoch()).isEqualTo(0L);
        assertThatThrownBy(() -> runtime.getLayoutView()
                                             .getRuntimeLayout()
                                             .getBaseClient(singleNodeEndpoint)
                                             .ping()
                                             .get())
                                             .hasRootCauseInstanceOf(TimeoutException.class);
        assertThat(shutdownCorfuServer(corfuServer)).isTrue();
    }

    /**
     * {@link MsgHandlingFilter} provides a collection of convenience methods for quickly creating
     * message filters with particular dropping behaviors. This test enables filtering of
     * LAYOUT_RESPONSE {@link CorfuMsgType} and shows that randomly dropping 50% of such messages
     * won't affect the initialization of a {@link CorfuRuntime}. The filter handler is created by one of
     * convenience methods provided by {@link MsgHandlingFilter} and uses a uniformly distributed pseudo
     * random function for dropping the inbound LAYOUT_RESPONSE messages.
     *
     * @throws Exception
     */
    @Test
    public void test1NodeMsgFilterHelperSimulationRandomDropLayoutResponse() throws Exception {
        // Run a single persistent server
        Process corfuServer = runSinglePersistentServer(corfuSingleNodeHost, corfuSingleNodePort);

        // Create a collection of filters with one filter randomly dropping
        // 50% of LAYOUT_RESPONSE messages
        List<MsgHandlingFilter> handlingFilters = new ArrayList<>();
        final double droppingProbability = 0.5;

        MsgHandlingFilter filter =
                MsgHandlingFilter.msgDropUniformRandomFilterFor(
                        Collections.singleton(CorfuMsgType.LAYOUT_RESPONSE),
                        droppingProbability);
        handlingFilters.add(filter);

        // Build parameters for CorfuRuntime
        final CorfuRuntimeParameters.CorfuRuntimeParametersBuilder parametersBuilder =
                CorfuRuntime.CorfuRuntimeParameters.builder();
        parametersBuilder.nettyClientInboundMsgFilters(handlingFilters);
        final CorfuRuntimeParameters corfuRuntimeParameters = parametersBuilder.build();

        // Initialize and connect to a corfuRuntime
        runtime = CorfuRuntime
                .fromParameters(corfuRuntimeParameters)
                .parseConfigurationString(singleNodeEndpoint)
                .connect();

        // Verify
        assertThat(runtime.getLayoutView().getLayout().getEpoch()).isEqualTo(0L);
        assertThat(runtime.getLayoutView()
                               .getRuntimeLayout()
                               .getBaseClient(singleNodeEndpoint)
                               .getVersionInfo().get())
                               .isNotNull();
        assertThat(shutdownCorfuServer(corfuServer)).isTrue();
    }

    /**
     * This test represents adding a filter that allows all messages
     *
     * @throws Exception
     */
    @Test
    public void test1NodeSimulationAllowAll() throws Exception {
        // Run a single persistent server
        Process corfuServer = runSinglePersistentServer(corfuSingleNodeHost, corfuSingleNodePort);

        // Create a collection of filters with a filter allowing all messages
        List<MsgHandlingFilter> handlingFilters = new ArrayList<>();
        final Function<CorfuMsg, CorfuMsg> allowAll = a -> a;

        MsgHandlingFilter filter = new MsgHandlingFilter(a -> true,
                                                         allowAll);
        handlingFilters.add(filter);

        // Build parameters for CorfuRuntime
        final CorfuRuntimeParameters.CorfuRuntimeParametersBuilder parametersBuilder =
                CorfuRuntime.CorfuRuntimeParameters.builder();
        parametersBuilder.nettyClientInboundMsgFilters(handlingFilters);
        final CorfuRuntimeParameters corfuRuntimeParameters = parametersBuilder.build();

        // Initialize and connect to a corfuRuntime
        runtime = CorfuRuntime
                .fromParameters(corfuRuntimeParameters)
                .parseConfigurationString(singleNodeEndpoint)
                .connect();

        // Verify
        assertThat(runtime.getLayoutView().getLayout().getEpoch()).isEqualTo(0L);
        assertThat(runtime.getLayoutView()
                .getRuntimeLayout()
                .getBaseClient(singleNodeEndpoint)
                .getVersionInfo().get())
                .isNotNull();
        assertThat(shutdownCorfuServer(corfuServer)).isTrue();
    }

    /**
     * This test represents adding a disabled filter for dropping all messages
     * will not effect the messages
     *
     * @throws Exception
     */
    @Test
    public void test1NodeSimulationDisabledDropAll() throws Exception {
        // Run a single persistent server
        Process corfuServer = runSinglePersistentServer(corfuSingleNodeHost, corfuSingleNodePort);

        // Create a collection of filters with a disabled filter.
        List<MsgHandlingFilter> handlingFilters = new ArrayList<>();
        final Function<CorfuMsg, CorfuMsg> dropAll = a -> null;

        MsgHandlingFilter filter = new MsgHandlingFilter(a -> false,
                                                         dropAll);
        handlingFilters.add(filter);

        // Build parameters for CorfuRuntime
        final CorfuRuntimeParameters.CorfuRuntimeParametersBuilder parametersBuilder =
                CorfuRuntime.CorfuRuntimeParameters.builder();
        parametersBuilder.nettyClientInboundMsgFilters(handlingFilters);
        final CorfuRuntimeParameters corfuRuntimeParameters = parametersBuilder.build();

        // Initialize and connect to a corfuRuntime
        runtime = CorfuRuntime
                .fromParameters(corfuRuntimeParameters)
                .parseConfigurationString(singleNodeEndpoint)
                .connect();

        // Verify
        assertThat(runtime.getLayoutView().getLayout().getEpoch()).isEqualTo(0L);
        assertThat(runtime.getLayoutView()
                .getRuntimeLayout()
                .getBaseClient(singleNodeEndpoint)
                .getVersionInfo().get())
                .isNotNull();
        assertThat(shutdownCorfuServer(corfuServer)).isTrue();
    }
}
