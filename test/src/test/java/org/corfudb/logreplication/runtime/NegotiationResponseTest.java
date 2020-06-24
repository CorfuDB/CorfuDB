package org.corfudb.logreplication.runtime;

import org.corfudb.infrastructure.LogReplicationRuntimeParameters;
import org.corfudb.infrastructure.logreplication.receive.LogReplicationMetadataManager;
import org.corfudb.logreplication.fsm.LogReplicationEvent;
import org.corfudb.logreplication.infrastructure.LogReplicationNegotiationException;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationNegotiationResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.BDDMockito.given;

@SuppressWarnings("checkstyle:magicnumber")
public class NegotiationResponseTest {
    private LogReplicationRuntimeParameters parameters;
    private LogReplicationMetadataManager metadataManager;
    private CorfuRuntime corfuRuntime;
    private static final String version = "version";
    private static final long siteConfigId = 0L;

    CorfuLogReplicationRuntime replicationRuntime;

    @Before
    public void setup() {
        // Generate dependencies for CorfuReplicationRuntime
        parameters = LogReplicationRuntimeParameters.builder()
                .build();
        metadataManager = mock(LogReplicationMetadataManager.class);
        corfuRuntime = mock(CorfuRuntime.class, RETURNS_DEEP_STUBS);

        int topologyConfigId = 0;

        replicationRuntime = new CorfuLogReplicationRuntime(parameters,
                metadataManager, corfuRuntime, topologyConfigId);
    }

    @Test
    public void verifyVersionCheck() throws Exception {
        LogReplicationNegotiationResponse negotiationResponse = LogReplicationNegotiationResponse
                .builder()
                .version("version_0")
                .build();
        doReturn(version).when(metadataManager).getVersion();
        String expectedMessage = getMessage("Mismatch of version number");
        assertThatThrownBy(() -> replicationRuntime.processNegotiationResponse(negotiationResponse))
                .isInstanceOf(LogReplicationNegotiationException.class)
                .hasMessage(expectedMessage);
    }

    @Test
    public void verifySmallerSiteConfigIdCheck() throws Exception {
        LogReplicationNegotiationResponse negotiationResponse = LogReplicationNegotiationResponse
                .builder()
                .version(version)
                .siteConfigID(0L)
                .build();
        doReturn(version).when(metadataManager).getVersion();
        doReturn(1L).when(metadataManager).getSiteConfigID();
        String expectedMessage = getMessage("Mismatch of configID");
        assertThatThrownBy(() -> replicationRuntime.processNegotiationResponse(negotiationResponse))
                .isInstanceOf(LogReplicationNegotiationException.class)
                .hasMessage(expectedMessage);
    }

    @Test
    public void verifyGreaterSiteConfigIdCheck() throws Exception {
        LogReplicationNegotiationResponse negotiationResponse = LogReplicationNegotiationResponse
                .builder()
                .siteConfigID(1L)
                .version(version)
                .build();
        doReturn(version).when(metadataManager).getVersion();
        doReturn(0L).when(metadataManager).getSiteConfigID();
        String expectedMessage = getMessage("Mismatch of configID");
        assertThatThrownBy(() -> replicationRuntime.processNegotiationResponse(negotiationResponse))
                .isInstanceOf(LogReplicationNegotiationException.class)
                .hasMessage(expectedMessage);
    }

    @Test
    public void verifyFreshStart() throws Exception {
        LogReplicationNegotiationResponse negotiationResponse = LogReplicationNegotiationResponse
                .builder()
                .version(version)
                .siteConfigID(siteConfigId)
                .snapshotStart(-1L)
                .build();
        doReturn(version).when(metadataManager).getVersion();
        doReturn(siteConfigId).when(metadataManager).getSiteConfigID();
        given(corfuRuntime.getAddressSpaceView().getTrimMark().getSequence()).willReturn(0L);
        assertThat(replicationRuntime.processNegotiationResponse(negotiationResponse).getType())
                .isEqualTo(LogReplicationEvent.LogReplicationEventType.SNAPSHOT_SYNC_REQUEST);
    }

    @Test
    public void verifyFullSyncPhaseI() throws Exception {
        LogReplicationNegotiationResponse negotiationResponse = LogReplicationNegotiationResponse
                .builder()
                .version(version)
                .siteConfigID(siteConfigId)
                .snapshotStart(100L)
                .snapshotTransferred(-1L)
                .build();
        doReturn(version).when(metadataManager).getVersion();
        doReturn(siteConfigId).when(metadataManager).getSiteConfigID();
        given(corfuRuntime.getAddressSpaceView().getTrimMark().getSequence()).willReturn(0L);
        assertThat(replicationRuntime.processNegotiationResponse(negotiationResponse).getType())
                .isEqualTo(LogReplicationEvent.LogReplicationEventType.SNAPSHOT_SYNC_REQUEST);
    }

    @Test
    public void verifyFullSyncPhaseII() throws Exception {
        LogReplicationNegotiationResponse negotiationResponse = LogReplicationNegotiationResponse
                .builder()
                .version(version)
                .siteConfigID(siteConfigId)
                .snapshotStart(100L)
                .snapshotTransferred(100L)
                .snapshotApplied(-1L)
                .build();
        doReturn(version).when(metadataManager).getVersion();
        doReturn(siteConfigId).when(metadataManager).getSiteConfigID();
        given(corfuRuntime.getAddressSpaceView().getTrimMark().getSequence()).willReturn(0L);
        LogReplicationEvent event = replicationRuntime.processNegotiationResponse(negotiationResponse);
        assertThat(event.getType()).isEqualTo(
                LogReplicationEvent.LogReplicationEventType.SNAPSHOT_WAIT_COMPLETE);
        assertThat(event.getMetadata().getSyncTimestamp()).isEqualTo(negotiationResponse.getSnapshotStart());
    }

    @Test
    public void verifyLogEntryStateContinue() throws Exception {
        LogReplicationNegotiationResponse negotiationResponse = LogReplicationNegotiationResponse
                .builder()
                .version(version)
                .siteConfigID(siteConfigId)
                .snapshotStart(100L)
                .snapshotTransferred(100L)
                .snapshotApplied(100L)
                .lastLogProcessed(200L)
                .build();
        doReturn(version).when(metadataManager).getVersion();
        doReturn(siteConfigId).when(metadataManager).getSiteConfigID();
        given(corfuRuntime.getAddressSpaceView().getTrimMark().getSequence()).willReturn(0L);
        LogReplicationEvent event = replicationRuntime.processNegotiationResponse(negotiationResponse);
        assertThat(event.getType()).isEqualTo(
                LogReplicationEvent.LogReplicationEventType.REPLICATION_START);
        assertThat(event.getMetadata().getSyncTimestamp()).isEqualTo(negotiationResponse.getLastLogProcessed());
    }

    @Test
    public void verifyLogEntryStateBreak() throws Exception {
        LogReplicationNegotiationResponse negotiationResponse = LogReplicationNegotiationResponse
                .builder()
                .version(version)
                .siteConfigID(siteConfigId)
                .snapshotStart(100L)
                .snapshotTransferred(100L)
                .snapshotApplied(100L)
                .lastLogProcessed(200L)
                .build();
        doReturn(version).when(metadataManager).getVersion();
        doReturn(siteConfigId).when(metadataManager).getSiteConfigID();
        given(corfuRuntime.getAddressSpaceView().getTrimMark().getSequence()).willReturn(300L);
        LogReplicationEvent event = replicationRuntime.processNegotiationResponse(negotiationResponse);
        assertThat(event.getType()).isEqualTo(
                LogReplicationEvent.LogReplicationEventType.SNAPSHOT_SYNC_REQUEST);
        assertThat(event.getMetadata().getSyncTimestamp()).isEqualTo(-1L);
    }

    @Test
    public void verifyDefaultCase() throws Exception {
        LogReplicationNegotiationResponse negotiationResponse = LogReplicationNegotiationResponse
                .builder()
                .version(version)
                .siteConfigID(siteConfigId)
                .snapshotStart(100L)
                .snapshotTransferred(500L)
                .snapshotApplied(0L)
                .lastLogProcessed(-1L)
                .build();
        doReturn(version).when(metadataManager).getVersion();
        doReturn(siteConfigId).when(metadataManager).getSiteConfigID();
        given(corfuRuntime.getAddressSpaceView().getTrimMark().getSequence()).willReturn(0L);
        LogReplicationEvent event = replicationRuntime.processNegotiationResponse(negotiationResponse);
        assertThat(event.getType()).isEqualTo(
                LogReplicationEvent.LogReplicationEventType.SNAPSHOT_SYNC_REQUEST);
    }

    private String getMessage(String reason) {
        return String.format("Negotiation failed due to %s", reason);
    }
}
