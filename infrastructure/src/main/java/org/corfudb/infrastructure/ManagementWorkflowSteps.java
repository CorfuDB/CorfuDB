package org.corfudb.infrastructure;

import com.google.common.collect.Sets;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.infrastructure.log.StreamLogFiles;
import org.corfudb.protocols.wireprotocol.FileSegmentReplicationRequest;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.IClientRouter;
import org.corfudb.runtime.clients.LayoutClient;
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.runtime.clients.ManagementClient;
import org.corfudb.runtime.view.Layout;
import org.corfudb.workflow.Step;

@Slf4j
public class ManagementWorkflowSteps {

    public static CorfuRuntime corfuRuntime;
    public static FailureHandlerDispatcher failureHandlerDispatcher;

    public enum ManagementSteps {

        SET_UP_NODE(setUpNewNode()),
        ADD_NODE(addNode()),
        SEGMENT_CATCHUP(segmentCatchup()),
        SEGMENT_REPLICATION(segmentReplication()),
        MERGE_SEGMENT(mergeSegment());

        @Getter
        private final Step step;

        ManagementSteps(Step step) {
            this.step = step;
        }
    }

    public static Step<Map<String, String>, Boolean> setUpNewNode() {
        return new Step<>("SET_UP_NODE", map -> {

            String endpoint = map.get("endpoint");

            // Bootstrap the to-be added node with the old layout.
            IClientRouter newEndpointRouter = corfuRuntime.getRouter(endpoint);
            try {
                Layout layout = (Layout) corfuRuntime.getLayoutView().getLayout().clone();
                // Ignoring call result as the call returns ACK or throws an exception.
                newEndpointRouter.getClient(LayoutClient.class)
                        .bootstrapLayout(layout).get();
                newEndpointRouter.getClient(ManagementClient.class)
                        .bootstrapManagement(layout).get();
                newEndpointRouter.getClient(ManagementClient.class)
                        .initiateFailureHandler().get();

                log.info("handleAddNodeRequest: New node {} bootstrapped.", endpoint);
            } catch (Exception e) {
                log.error("handleAddNodeRequest: "
                        + "Aborting as new node could not be bootstrapped : ", e);
                return false;
            }
            return true;
        });
    }

    public static Step<Map<String, String>, Boolean> addNode() {
        return new Step<>("ADD_NODE", map -> {

            Layout layout = corfuRuntime.getLayoutView().getLayout();
            String endpoint = map.get("endpoint");
            boolean isLayoutServer = Boolean.parseBoolean(map.get("isLayoutServer"));
            boolean isSequencerServer = Boolean.parseBoolean(map.get("isSequencerServer"));
            boolean isLogUnitServer = Boolean.parseBoolean(map.get("isLogUnitServer"));
            boolean isUnresponsiveServer = Boolean.parseBoolean(map.get("isUnresponsiveServer"));
            int logUnitStripeIndex = Integer.parseInt(map.get("logUnitStripeIndex"));

            try {
                // Fill layout slot if failure in maxGlobalLogTail
                // Not wait for another failure detector.
                failureHandlerDispatcher.addNode(layout, corfuRuntime, endpoint, isLayoutServer,
                        isSequencerServer, isLogUnitServer, isUnresponsiveServer,
                        logUnitStripeIndex);
            } catch (Exception e) {
                log.error("Request to add new node: {} failed with exception:", map, e);
                return false;
            }
            return true;
        });
    }

    public static Step<Map<String, String>, Boolean> segmentCatchup() {
        return new Step<>("SEGMENT_CATCHUP", map -> {

            Layout.LayoutSegment segment =
                    Layout.getParser().fromJson(map.get("segment"), Layout.LayoutSegment.class);

            log.info("Starting hole filling.");

            // TODO: Abstract this logic to replication mode specific segment merging.
            // Enabling merge segments only for chain replication.
            if (!segment.getReplicationMode().equals(Layout.ReplicationMode.CHAIN_REPLICATION)) {
                throw new UnsupportedOperationException(
                        "Segment catchup only implemented for chain replication.");
            }

            // Catchup segment for every stripe.
            for (Layout.LayoutStripe layoutStripe : segment.getStripes()) {
                List<String> logServers = layoutStripe.getLogServers();

                if (logServers.size() < 2) {
                    log.info("Hole filling not required as only one log server present in stripe.");
                    continue;
                }

                // Chain replication specific hole filling mechanism.
                Set<Long> headKnownAddressSet;
                Set<Long> tailKnownAddressSet;

                try {
                    // Fetch known address set from head.
                    headKnownAddressSet = corfuRuntime
                            .getRouter(logServers.get(0))
                            .getClient(LogUnitClient.class)
                            .requestKnownAddressSet(segment.getStart(), segment.getEnd()).get();
                    // Fetch known address set from tail.
                    tailKnownAddressSet = corfuRuntime
                            .getRouter(logServers.get(logServers.size() - 1))
                            .getClient(LogUnitClient.class)
                            .requestKnownAddressSet(segment.getStart(), segment.getEnd()).get();

                } catch (InterruptedException | ExecutionException e) {
                    log.error("Unable to fetch known address set from log units, ", e);
                    throw new RuntimeException(e);
                }

                // Diff known address sets.
                Set<Long> differenceSet = Sets.difference(headKnownAddressSet, tailKnownAddressSet)
                        .immutableCopy();

                if (differenceSet.isEmpty()) {
                    log.info("Head and tail log servers in the chain have given segment replicated.");
                    continue;
                }

                // Trigger reads on tail for result addresses to hole fill differences.
                corfuRuntime.getAddressSpaceView().read(differenceSet);
            }
            log.info("Segment catchup completed.");

            return true;
        });
    }

    public static Step<Map<String, String>, Boolean> segmentReplication() {
        return new Step<>("SEGMENT_REPLICATION", map -> {

            Layout.LayoutSegment segment =
                    Layout.getParser().fromJson(map.get("segment"), Layout.LayoutSegment.class);
            String newEndpoint = map.get("newEndpoint");
            String logPath = map.get("logPath");

            LogUnitClient logUnitClient = corfuRuntime.getRouter(newEndpoint)
                    .getClient(LogUnitClient.class);

            try {
                long logFileStartSegment =
                        segment.getStart() / StreamLogFiles.RECORDS_PER_LOG_FILE;
                long logFileEndSegment =
                        segment.getEnd() / StreamLogFiles.RECORDS_PER_LOG_FILE;

                log.info("Replicating files from:{} to:{}", logFileStartSegment, logFileEndSegment);
                for (long i = logFileStartSegment; i <= logFileEndSegment; i++) {
                    // Stream files directly as they are in the committed set.
                    String filePath = logPath + File.separator + "log" + File.separator + i
                            + ".log";

                    log.info("Replicating file: {}", filePath);

                    File segmentFile = new File(filePath);
                    FileInputStream fis = new FileInputStream(segmentFile);
                    byte[] fileBuffer = new byte[(int) segmentFile.length()];
                    fis.read(fileBuffer);
                    // We pay the cost of byte[] copy.
                    logUnitClient.replicateSegment(i, fileBuffer, true).get();
                }

            } catch (InterruptedException | ExecutionException | IOException e) {
                log.error("ChainReplication: Segment replication failed, ", e);
                throw new RuntimeException(e);
            }
            return true;
        });
    }

    public static Step<Map<String, String>, Boolean> mergeSegment() {
        return new Step<>("MERGE_SEGMENT", o -> {
            try {
                Layout layout = (Layout) corfuRuntime.getLayoutView().getLayout().clone();
                failureHandlerDispatcher.mergeSegments(layout, corfuRuntime);
            } catch (Exception e) {
                log.error("Request to merge segments failed with exception:", e);
                throw new RuntimeException(e);
            }
            return true;
        });
    }
}
