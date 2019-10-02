package org.corfudb.integration;

import static org.assertj.core.api.Assertions.assertThat;

import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.stream.IStreamView;
import org.corfudb.test.CorfuServerRunner;
import org.junit.Ignore;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.UUID;

/**
 * Tests for clojure cmdlets.
 * TODO: Implement few tests with TLS options too.
 * FIXME: Cmdlets logunit read do not work if serverEpoch > 1
 * <p>
 * Created by zlokhandwala on 5/8/17.
 */
@Ignore
public class CmdletIT extends AbstractIT {

    // Using port 9901 to avoid intellij port conflict.
    private final int PORT = 9901;
    private final String ENDPOINT = DEFAULT_HOST + ":" + PORT;

    private Layout getSingleLayout() {
        return new Layout(
                Collections.singletonList(ENDPOINT),
                Collections.singletonList(ENDPOINT),
                Collections.singletonList(new Layout.LayoutSegment(Layout.ReplicationMode.CHAIN_REPLICATION,
                        0L,
                        -1L,
                        Collections.singletonList(new Layout.LayoutStripe(Collections.singletonList(ENDPOINT))))),
                Collections.EMPTY_LIST,
                0L,
                UUID.randomUUID());
    }

    static public String runCmdletGetOutput(String command) throws Exception {
        ProcessBuilder builder = new ProcessBuilder("sh", "-c", command);
        builder.redirectErrorStream(true);
        Process cmdlet = builder.start();
        final StringBuilder output = new StringBuilder();

        try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(cmdlet.getInputStream()))) {
            String line = null;
            while ((line = bufferedReader.readLine()) != null) {
                output.append(line);
            }
            cmdlet.waitFor();
        }
        return output.toString();
    }

    private Process corfuServerProcess;

    /**
     * Testing corfu_layout.
     *
     * @throws Exception
     */
    @Test
    public void testCorfuLayoutCmdlet() throws Exception {

        corfuServerProcess = new CorfuServerRunner().setPort(PORT).runServer();
        final String command = CORFU_PROJECT_DIR + "bin/corfu_layout " + ENDPOINT;
        assertThat(runCmdletGetOutput(command).contains(getSingleLayout().toString()))
                .isTrue();
        shutdownCorfuServer(corfuServerProcess);
    }

    /**
     * Testing corfu_ping
     *
     * @throws Exception
     */
    @Test
    public void testCorfuPingCmdlet() throws Exception {

        corfuServerProcess = new CorfuServerRunner().setPort(PORT).runServer();
        final String command = CORFU_PROJECT_DIR + "bin/corfu_ping " + ENDPOINT;
        final String expectedSubString = "PING " + ENDPOINT + "ACK";
        assertThat(runCmdletGetOutput(command).contains(expectedSubString))
                .isTrue();
        shutdownCorfuServer(corfuServerProcess);
    }

    /**
     * Testing corfu_layouts query
     * TODO: Not testing corfu_layouts -c <endpoint> edit
     *
     * @throws Exception
     */
    @Test
    public void testCorfuLayoutsCmdlet() throws Exception {

        corfuServerProcess = new CorfuServerRunner().setPort(PORT).runServer();
        final String command = CORFU_PROJECT_DIR + "bin/corfu_layouts -c " + ENDPOINT + " query";
        // Squashing all spaces to compare JSON.
        assertThat(runCmdletGetOutput(command).replace(" ", "")
                .contains(getSingleLayout().asJSONString()))
                .isTrue();
        shutdownCorfuServer(corfuServerProcess);
    }

    /**
     * Testing corfu_query
     *
     * @throws Exception
     */
    @Test
    public void testCorfuQueryCmdlet() throws Exception {

        corfuServerProcess = new CorfuServerRunner()
                .setPort(PORT)
                .setLogPath(CorfuServerRunner.getCorfuServerLogPath(DEFAULT_HOST, PORT))
                .runServer();

        final String command = CORFU_PROJECT_DIR + "bin/corfu_query " + ENDPOINT;
        final String expectedLogPath = "--log-path=" + CORFU_LOG_PATH;
        final String expectedInitialToken = "--initial-token=-1";
        final String expectedStartupArgs = new CorfuServerRunner()
                .setPort(PORT)
                .setLogPath(CorfuServerRunner.getCorfuServerLogPath(DEFAULT_HOST, PORT))
                .getOptionsString();
        String output = runCmdletGetOutput(command);
        assertThat(output.contains(expectedLogPath)).isTrue();
        assertThat(output.contains(expectedInitialToken)).isTrue();
        assertThat(output.contains(expectedStartupArgs)).isTrue();
        shutdownCorfuServer(corfuServerProcess);
    }

    /**
     * Testing corfu_stream append and read.
     *
     * @throws Exception
     */
    @Test
    public void testCorfuStreamCmdlet() throws Exception {

        corfuServerProcess = new CorfuServerRunner().setPort(PORT).runServer();
        final String streamA = "streamA";
        runtime = createRuntime(ENDPOINT);
        IStreamView streamViewA = runtime.getStreamsView().get(CorfuRuntime.getStreamID(streamA));

        String payload1 = "Hello";
        streamViewA.append(payload1.getBytes());

        String commandRead = CORFU_PROJECT_DIR + "bin/corfu_stream -i " + streamA + " -c " + ENDPOINT + " read";
        String output = runCmdletGetOutput(commandRead);
        assertThat(output.contains(payload1)).isTrue();

        String payload2 = "World";
        String commandAppend = "echo '" + payload2 + "' | " + CORFU_PROJECT_DIR + "bin/corfu_stream -i " + streamA + " -c " + ENDPOINT + " append";
        runCmdletGetOutput(commandAppend);

        assertThat(streamViewA.next().getPayload(runtime)).isEqualTo(payload1.getBytes());
        assertThat(streamViewA.next().getPayload(runtime)).isEqualTo((payload2 + "\n").getBytes());
        assertThat(streamViewA.next()).isNull();
        shutdownCorfuServer(corfuServerProcess);
    }

    /**
     * Testing corfu_sequencer next-token and latest
     *
     * @throws Exception
     */
    @Test
    public void testCorfuSequencerCmdlet() throws Exception {

        corfuServerProcess = new CorfuServerRunner().setPort(PORT).runServer();
        final String streamA = "streamA";
        runtime = createRuntime(ENDPOINT);

        String commandNextToken = CORFU_PROJECT_DIR + "bin/corfu_sequencer -i " + streamA + " -c " + ENDPOINT + " next-token 3";
        runCmdletGetOutput(commandNextToken);

        Token token = runtime.getSequencerView().query().getToken();

        String commandLatest = CORFU_PROJECT_DIR + "bin/corfu_sequencer -i " + streamA + " -c " + ENDPOINT + " latest";
        String output = runCmdletGetOutput(commandLatest);
        assertThat(output.contains(token.toString())).isTrue();
        shutdownCorfuServer(corfuServerProcess);
    }

    @Test
    public void testCorfuAddressSpaceCmdlet() throws Exception {

        corfuServerProcess = new CorfuServerRunner().setPort(PORT).runServer();
        final String streamA = "streamA";
        String payload = "Hello";
        String commandAppend = "echo '" + payload + "' | " + CORFU_PROJECT_DIR + "bin/corfu_as -i " + streamA + " -c " + ENDPOINT + " write 0";
        runCmdletGetOutput(commandAppend);
        String commandRead = CORFU_PROJECT_DIR + "bin/corfu_as -i " + streamA + " -c " + ENDPOINT + " read 0";
        assertThat(runCmdletGetOutput(commandRead).contains(payload)).isTrue();
        shutdownCorfuServer(corfuServerProcess);
    }

    @Test
    public void testCorfuHandleFailuresCmdlet() throws Exception {

        corfuServerProcess = new CorfuServerRunner().setPort(PORT).runServer();
        final String command = CORFU_PROJECT_DIR + "bin/corfu_handle_failures -c " + ENDPOINT;
        final String expectedSubString = "Failure handler on " + ENDPOINT + " started.Initiation completed !";
        assertThat(runCmdletGetOutput(command).contains(expectedSubString))
                .isTrue();
        shutdownCorfuServer(corfuServerProcess);
    }

    @Test
    public void testCorfuLogunitCmdlet() throws Exception {

        corfuServerProcess = new CorfuServerRunner().setPort(PORT).runServer();
        String payload = "Hello";
        String commandAppend = "echo '" + payload + "' | " + CORFU_PROJECT_DIR + "bin/corfu_logunit " + ENDPOINT + " write 0";
        runCmdletGetOutput(commandAppend);

        String commandRead = CORFU_PROJECT_DIR + "bin/corfu_logunit " + ENDPOINT + " read 0";
        assertThat(runCmdletGetOutput(commandRead).contains(payload)).isTrue();
        shutdownCorfuServer(corfuServerProcess);
    }

    @Test
    public void testCorfuResetCmdlet() throws Exception {

        corfuServerProcess = new CorfuServerRunner().setPort(PORT).runServer();
        final String expectedOutput = "Reset " + ENDPOINT + ":ACK";
        String commandRead = CORFU_PROJECT_DIR + "bin/corfu_reset " + ENDPOINT;
        assertThat(runCmdletGetOutput(commandRead).contains(expectedOutput)).isTrue();
        shutdownCorfuServer(corfuServerProcess);
    }

    @Test
    public void testCorfuBootstrapCluster() throws Exception {
        corfuServerProcess = new CorfuServerRunner()
                .setPort(PORT)
                .setSingle(false)
                .runServer();
        File layoutFile = new File(CORFU_LOG_PATH + File.separator + "layoutFile");
        layoutFile.createNewFile();
        try (FileOutputStream fos = new FileOutputStream(layoutFile)) {
            fos.write(getSingleLayout().asJSONString().getBytes());
        }
        String command = CORFU_PROJECT_DIR + "bin/corfu_bootstrap_cluster -l " + layoutFile.getAbsolutePath();
        String expectedOutput = "New layout installed";

        assertThat(runCmdletGetOutput(command).contains(expectedOutput)).isTrue();
        shutdownCorfuServer(corfuServerProcess);
    }

    @Test
    public void testCorfuBootstrapClusterWithStream() throws Exception {
        corfuServerProcess = new CorfuServerRunner()
                .setPort(PORT)
                .setSingle(false)
                .runServer();
        File layoutFile = new File(CORFU_LOG_PATH + File.separator + "layoutFile");
        layoutFile.createNewFile();
        try (FileOutputStream fos = new FileOutputStream(layoutFile)) {
            fos.write(getSingleLayout().asJSONString().getBytes());
        }
        String command = CORFU_PROJECT_DIR + "bin/corfu_bootstrap_cluster -l " + layoutFile.getAbsolutePath();
        String expectedOutput = "New layout installed";

        assertThat(runCmdletGetOutput(command).contains(expectedOutput)).isTrue();

        runtime = createRuntime(ENDPOINT);
        IStreamView streamViewA = runtime.getStreamsView().get(CorfuRuntime.getStreamID("streamA"));
        assertThat(streamViewA.hasNext())
                .isFalse();

        shutdownCorfuServer(corfuServerProcess);
    }

    @Test
    public void testCorfuManagementBootstrap() throws Exception {
        corfuServerProcess = new CorfuServerRunner()
                .setPort(PORT)
                .setSingle(false)
                .runServer();
        File layoutFile = new File(CORFU_LOG_PATH + File.separator + "layoutFile");
        layoutFile.createNewFile();
        try (FileOutputStream fos = new FileOutputStream(layoutFile)) {
            fos.write(getSingleLayout().asJSONString().getBytes());
        }
        String command = CORFU_PROJECT_DIR + "bin/corfu_management_bootstrap -c " + ENDPOINT + " -l " + layoutFile.getAbsolutePath();
        String expectedOutput = ENDPOINT + " bootstrapped successfully";

        assertThat(runCmdletGetOutput(command).contains(expectedOutput)).isTrue();

        shutdownCorfuServer(corfuServerProcess);
    }

    @Test
    public void testCorfuSMRObject() throws Exception {
        corfuServerProcess = new CorfuServerRunner().setPort(PORT).runServer();
        String streamA = "streamA";
        String payload = "helloWorld";
        final String commandPut = CORFU_PROJECT_DIR + "bin/corfu_smrobject" +
                " -i " + streamA +
                " -c " + ENDPOINT +
                " " + SMRMap.class.getCanonicalName() + " putIfAbsent x " + payload;
        runCmdletGetOutput(commandPut);

        final String commandGet = CORFU_PROJECT_DIR + "bin/corfu_smrobject" +
                " -i " + streamA +
                " -c " + ENDPOINT +
                " " + SMRMap.class.getCanonicalName() + " getOrDefault x none";

        assertThat(runCmdletGetOutput(commandGet).contains(payload)).isTrue();

        shutdownCorfuServer(corfuServerProcess);
    }

}
