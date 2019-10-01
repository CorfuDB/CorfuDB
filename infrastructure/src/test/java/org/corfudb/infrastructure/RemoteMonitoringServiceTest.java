package org.corfudb.infrastructure;


import org.corfudb.infrastructure.management.ClusterStateContext;
import org.corfudb.infrastructure.management.FailureDetector;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.concurrent.SingletonResource;
import org.junit.Test;
import org.mockito.Mockito;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.corfudb.infrastructure.RemoteMonitoringService.DetectorTask.*;
import static org.corfudb.runtime.view.Layout.*;
import static org.corfudb.runtime.view.Layout.ReplicationMode.CHAIN_REPLICATION;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class RemoteMonitoringServiceTest extends LayoutBasedTest{

    @Test
    public void testRestoreRedundancyTrigger(){
        ServerContext serverContext = Mockito.mock(ServerContext.class);
        when(serverContext.getLocalEndpoint()).thenReturn("localhost");
        SingletonResource<CorfuRuntime> resource = (SingletonResource<CorfuRuntime>)
                Mockito.mock(SingletonResource.class);
        CorfuRuntime corfuRuntime = Mockito.mock(CorfuRuntime.class);
        when(resource.get()).thenReturn(corfuRuntime);
        FailureDetector failureDetector = Mockito.mock(FailureDetector.class);
        ClusterStateContext clusterStateContext = Mockito.mock(ClusterStateContext.class);
        LocalMonitoringService localMonitoringService = Mockito.mock(LocalMonitoringService.class);
        RemoteMonitoringService rms = new RemoteMonitoringService(serverContext, resource,
                clusterStateContext, failureDetector, localMonitoringService);

        List<LayoutStripe> presentStripes = Collections.singletonList
                (new LayoutStripe(Collections.singletonList("localhost")));

        LayoutSegment layoutSegment1 = new LayoutSegment(CHAIN_REPLICATION, 0L, 1L,
                presentStripes);

        List<LayoutSegment> presentSegments = Collections.singletonList(layoutSegment1);
        Layout layout = createTestLayout(presentSegments);
        RemoteMonitoringService spy = spy(rms);
        doReturn(true).when(spy)
                .handleMergeSegments(resource, layout, Duration.ofSeconds(1));

        // when node present in all segments, skipped.
        assertThat(rms.restoreRedundancy(layout)).isEqualTo(SKIPPED);

        // when node is not present in some segments, completed.
        LayoutStripe emptyStripe = new LayoutStripe(new ArrayList<>());
        List<LayoutStripe> someNonPresentStripes = Collections.singletonList(emptyStripe);

        LayoutSegment emptySegment = new LayoutSegment(CHAIN_REPLICATION, 1L, 2L,
                someNonPresentStripes);

        List<LayoutSegment> someNonPresentSegments = Arrays.asList(emptySegment, layoutSegment1);
        Layout layout2 = createTestLayout(someNonPresentSegments);
        assertThat(rms.restoreRedundancy(layout2)).isEqualTo(COMPLETED);

    }
}
