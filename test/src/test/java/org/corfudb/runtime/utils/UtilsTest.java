package org.corfudb.runtime.utils;

import com.google.common.collect.ImmutableMap;
import lombok.Data;
import org.corfudb.protocols.wireprotocol.StreamsAddressResponse;
import org.corfudb.protocols.wireprotocol.TailsResponse;
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.Layout.LayoutSegment;
import org.corfudb.runtime.view.Layout.LayoutStripe;
import org.corfudb.runtime.view.RuntimeLayout;
import org.corfudb.runtime.view.stream.StreamAddressSpace;
import org.corfudb.util.Utils;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.LongStream;

import static org.assertj.core.api.Java6Assertions.assertThat;
import static org.assertj.core.api.Java6Assertions.assertThatThrownBy;
import static org.corfudb.runtime.view.Layout.ReplicationMode.CHAIN_REPLICATION;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class UtilsTest {

  private static String nodeA = "nodeA";
  private static String nodeB = "nodeB";
  private static String nodeC = "nodeC";

  @Data
  class Context {
    private final Map<String, LogUnitClient> clientMap;
    private final RuntimeLayout runtimeLayout;

    public LogUnitClient getLogUnitClient(String node) {
      return clientMap.get(node);
    }
  }

  private Layout getLayout() {
    long epoch = 1;
    UUID uuid = UUID.randomUUID();
    LayoutStripe stripe = new LayoutStripe(Arrays.asList(nodeA, nodeB, nodeC));
    LayoutSegment segment =
            new LayoutSegment(CHAIN_REPLICATION, 0L, -1L, Collections.singletonList(stripe));

    return new Layout(
            Arrays.asList(nodeA, nodeB, nodeC),
            Arrays.asList(nodeA, nodeB, nodeC),
            Collections.singletonList(segment),
            Collections.emptyList(),
            Collections.emptyList(),
            epoch,
            uuid
    );
  }

  @SuppressWarnings("checkstyle:magicnumber")
  private Layout getSegmentedLayout() {
    long epoch = 1;
    UUID uuid = UUID.randomUUID();
    LayoutStripe seg1Strip = new LayoutStripe(Arrays.asList(nodeA));
    LayoutStripe seg2Strip = new LayoutStripe(Arrays.asList(nodeA, nodeB));
    LayoutStripe seg3Strip = new LayoutStripe(Arrays.asList(nodeB, nodeA));

    LayoutSegment segment1 =
            new LayoutSegment(
                    CHAIN_REPLICATION, 0L, 100L, Collections.singletonList(seg1Strip));
    LayoutSegment segment2 =
            new LayoutSegment(
                    CHAIN_REPLICATION, 100L, 200L, Collections.singletonList(seg2Strip));
    LayoutSegment segment3 =
            new LayoutSegment(
                    CHAIN_REPLICATION, 200L, -1L, Collections.singletonList(seg3Strip));
    return new Layout(
            Arrays.asList(nodeA, nodeB, nodeC),
            Arrays.asList(nodeA, nodeB, nodeC),
            Arrays.asList(segment1, segment2, segment3),
            Collections.singletonList(nodeC),
            Collections.emptyList(),
            epoch,
            uuid);
  }

  @SuppressWarnings("checkstyle:magicnumber")
  private Context getContext(Layout layout) {
    RuntimeLayout runtimeLayout = mock(RuntimeLayout.class);
    when(runtimeLayout.getLayout()).thenReturn(layout);
    Map<String, LogUnitClient> clientMap = new HashMap<>();
    layout.getAllLogServers().forEach(lu -> clientMap.put(lu, mock(LogUnitClient.class)));

    when(runtimeLayout.getLogUnitClient(anyString()))
            .then(
                    invokation -> {
                      String node = (String) invokation.getArguments()[0];
                      if (!clientMap.containsKey(node)) {
                        throw new IllegalStateException("not expecting a call to " + node);
                      }
                      return clientMap.get(node);
                    });

    return new Context(clientMap, runtimeLayout);
  }

  private CompletableFuture<TailsResponse> getTailsResponse(
          long globalTail, Map<UUID, Long> streamTails, long epoch) {
    CompletableFuture<TailsResponse> cf = new CompletableFuture<>();
    TailsResponse resp = new TailsResponse(globalTail, streamTails);
    resp.setEpoch(epoch);
    cf.complete(resp);
    return cf;
  }

  private CompletableFuture<StreamsAddressResponse> getLogAddressSpaceResponse(
          long globalTail, Map<UUID, StreamAddressSpace> streamTails, long epoch) {
    CompletableFuture<StreamsAddressResponse> cf = new CompletableFuture<>();
    StreamsAddressResponse resp = new StreamsAddressResponse(globalTail, streamTails);
    resp.setEpoch(epoch);
    // TODO(Maithem) add epoch?
    cf.complete(resp);
    return cf;
  }

  private CompletableFuture<TailsResponse> getTailsResponse(long globalTail, long epoch) {
    return getTailsResponse(globalTail, Collections.EMPTY_MAP, epoch);
  }

  @Test
  @SuppressWarnings("checkstyle:magicnumber")
  public void getLogTailSingleSegmentTest() {
    Layout layout = getLayout();
    Context ctx = getContext(layout);
    final long globalTail = 5;

    when(ctx.getLogUnitClient(nodeA).getLogTail())
            .thenReturn(getTailsResponse(globalTail, layout.getEpoch()));

    long logTail = Utils.getLogTail(ctx.getRuntimeLayout());
    assertThat(logTail).isEqualTo(globalTail);
    verify(ctx.getLogUnitClient(nodeA), times(1)).getLogTail();
    verify(ctx.getRuntimeLayout(), times(1)).getLogUnitClient(nodeA);
    verify(ctx.getRuntimeLayout(), times(0)).getLogUnitClient(nodeB);
    verify(ctx.getRuntimeLayout(), times(0)).getLogUnitClient(nodeC);

    final long invalidEpoch = layout.getEpoch() + 1;
    when(ctx.getLogUnitClient(nodeA).getLogTail())
            .thenReturn(getTailsResponse(globalTail, invalidEpoch));
    assertThatThrownBy(() -> Utils.getLogTail(ctx.getRuntimeLayout()))
            .isInstanceOf(WrongEpochException.class);
  }

  @Test
  @SuppressWarnings("checkstyle:magicnumber")
  public void getLogTailMultiSegmentTest() {
    Layout layout = getSegmentedLayout();
    Context ctx = getContext(layout);

    final long nodeAGlobalTail = 5;
    final long nodeBGlobalTail = 150;
    when(ctx.getLogUnitClient(nodeA).getLogTail())
            .thenReturn(getTailsResponse(nodeAGlobalTail, layout.getEpoch()));
    when(ctx.getLogUnitClient(nodeB).getLogTail())
            .thenReturn(getTailsResponse(nodeBGlobalTail, layout.getEpoch()));

    final long logTail = Utils.getLogTail(ctx.getRuntimeLayout());
    assertThat(logTail).isEqualTo(nodeBGlobalTail);
    verify(ctx.getLogUnitClient(nodeA), times(1)).getLogTail();
    verify(ctx.getRuntimeLayout(), times(1)).getLogUnitClient(nodeA);
    // Since we coalesce the same node on multiple segments and nodeB is the head of segment2
    // and segment3 we need to verify that it is only queried once
    verify(ctx.getLogUnitClient(nodeB), times(1)).getLogTail();
    verify(ctx.getRuntimeLayout(), times(1)).getLogUnitClient(nodeB);
    verify(ctx.getRuntimeLayout(), times(0)).getLogUnitClient(nodeC);

    when(ctx.getLogUnitClient(nodeA).getLogTail())
            .thenReturn(getTailsResponse(nodeAGlobalTail, layout.getEpoch() + 1));
    assertThatThrownBy(() -> Utils.getLogTail(ctx.getRuntimeLayout()))
            .isInstanceOf(WrongEpochException.class);
  }

  @Test
  @SuppressWarnings("checkstyle:magicnumber")
  public void getAllTailsSingleSegmentTest() {
    Layout layout = getLayout();
    Context ctx = getContext(layout);
    final long globalTail = 20;

    Map<UUID, Long> streamTails =
            ImmutableMap.of(UUID.randomUUID(), 5L, UUID.randomUUID(), 10L, UUID.randomUUID(), 15L);

    when(ctx.getLogUnitClient(nodeA).getAllTails())
            .thenReturn(getTailsResponse(globalTail, streamTails, layout.getEpoch()));

    TailsResponse tails = Utils.getAllTails(ctx.getRuntimeLayout());
    assertThat(tails.getLogTail()).isEqualTo(globalTail);
    assertThat(tails.getStreamTails()).isEqualTo(streamTails);
    assertThat(tails.getEpoch()).isEqualTo(layout.getEpoch());
    verify(ctx.getLogUnitClient(nodeA), times(1)).getAllTails();
    verify(ctx.getRuntimeLayout(), times(1)).getLogUnitClient(nodeA);
    verify(ctx.getRuntimeLayout(), times(0)).getLogUnitClient(nodeB);
    verify(ctx.getRuntimeLayout(), times(0)).getLogUnitClient(nodeC);

    when(ctx.getLogUnitClient(nodeA).getAllTails())
            .thenReturn(getTailsResponse(globalTail, streamTails, layout.getEpoch() + 1));
    assertThatThrownBy(() -> Utils.getAllTails(ctx.getRuntimeLayout()))
            .isInstanceOf(WrongEpochException.class);
  }

  @Test
  @SuppressWarnings("checkstyle:magicnumber")
  public void getAllTailsMultiSegmentTest() {
    Layout layout = getSegmentedLayout();
    Context ctx = getContext(layout);
    final long nodeAGlobalTail = 15;
    final long nodeBGlobalTail = 215;

    UUID s1Id = UUID.randomUUID();
    UUID s2Id = UUID.randomUUID();
    UUID s3Id = UUID.randomUUID();
    UUID s4Id = UUID.randomUUID();

    Map<UUID, Long> nodeAStreamTails = ImmutableMap.of(s1Id, 5L, s2Id, 10L, s4Id, 12L);
    Map<UUID, Long> nodeBStreamTails = ImmutableMap.of(s1Id, 5L, s2Id, 103L, s3Id, 210L);

    when(ctx.getLogUnitClient(nodeA).getAllTails())
            .thenReturn(getTailsResponse(nodeAGlobalTail, nodeAStreamTails, layout.getEpoch()));

    when(ctx.getLogUnitClient(nodeB).getAllTails())
            .thenReturn(getTailsResponse(nodeBGlobalTail, nodeBStreamTails, layout.getEpoch()));

    TailsResponse tails = Utils.getAllTails(ctx.getRuntimeLayout());
    assertThat(tails.getLogTail()).isEqualTo(nodeBGlobalTail);
    assertThat(tails.getStreamTails())
            .isEqualTo(ImmutableMap.of(s1Id, 5L, s2Id, 103L, s3Id, 210L, s4Id, 12L));
    assertThat(tails.getEpoch()).isEqualTo(layout.getEpoch());
    verify(ctx.getLogUnitClient(nodeA), times(1)).getAllTails();
    verify(ctx.getRuntimeLayout(), times(1)).getLogUnitClient(nodeA);
    verify(ctx.getLogUnitClient(nodeB), times(1)).getAllTails();
    verify(ctx.getRuntimeLayout(), times(1)).getLogUnitClient(nodeB);
    verify(ctx.getRuntimeLayout(), times(0)).getLogUnitClient(nodeC);

    when(ctx.getLogUnitClient(nodeA).getAllTails())
            .thenReturn(getTailsResponse(nodeAGlobalTail, nodeAStreamTails, layout.getEpoch() + 1));
    assertThatThrownBy(() -> Utils.getAllTails(ctx.getRuntimeLayout()))
            .isInstanceOf(WrongEpochException.class);
  }

  private StreamAddressSpace getRandomStreamSpace(long max) {
    StreamAddressSpace streamA = new StreamAddressSpace(true);
    LongStream.range(0, max)
            .forEach(address -> streamA.addAddress(((address & 0x1) == 1) ? 0 : address));
    return streamA;
  }

  @Test
  @SuppressWarnings("checkstyle:magicnumber")
  public void getLogAddressSpaceSingleSegmentTest() {
    Layout layout = getLayout();
    Context ctx = getContext(layout);

    final long nodeAGlobalTail = 50;
    UUID s1Id = UUID.randomUUID();
    UUID s2Id = UUID.randomUUID();
    Map<UUID, StreamAddressSpace> nodeALogAddressSpace =
            ImmutableMap.of(
                    s1Id, getRandomStreamSpace(nodeAGlobalTail - 1),
                    s2Id, getRandomStreamSpace(nodeAGlobalTail - 1));

    when(ctx.getLogUnitClient(nodeA).getLogAddressSpace())
            .thenReturn(
                    getLogAddressSpaceResponse(nodeAGlobalTail, nodeALogAddressSpace, layout.getEpoch()));

    StreamsAddressResponse resp = Utils.getLogAddressSpace(ctx.getRuntimeLayout());
    assertThat(resp.getLogTail()).isEqualTo(nodeAGlobalTail);
    assertThat(resp.getAddressMap()).isEqualTo(nodeALogAddressSpace);
    verify(ctx.getLogUnitClient(nodeA), times(1)).getLogAddressSpace();
    verify(ctx.getRuntimeLayout(), times(1)).getLogUnitClient(nodeA);
    verify(ctx.getRuntimeLayout(), times(0)).getLogUnitClient(nodeB);
    verify(ctx.getRuntimeLayout(), times(0)).getLogUnitClient(nodeC);

    when(ctx.getLogUnitClient(nodeA).getLogAddressSpace())
            .thenReturn(
                    getLogAddressSpaceResponse(
                            nodeAGlobalTail, nodeALogAddressSpace, layout.getEpoch() + 1));
    assertThatThrownBy(() -> Utils.getLogAddressSpace(ctx.getRuntimeLayout()))
            .isInstanceOf(WrongEpochException.class);
  }

  @Test
  @SuppressWarnings("checkstyle:magicnumber")
  public void getLogAddressSpaceMultiSegmentTest() {
    Layout layout = getSegmentedLayout();
    Context ctx = getContext(layout);

    final long nodeAGlobalTail = 50;
    UUID s1Id = UUID.randomUUID();
    UUID s2Id = UUID.randomUUID();
    Map<UUID, StreamAddressSpace> nodeALogAddressSpace =
            ImmutableMap.of(
                    s1Id, getRandomStreamSpace(nodeAGlobalTail - 1),
                    s2Id, getRandomStreamSpace(nodeAGlobalTail - 1));

    UUID s3Id = UUID.randomUUID();
    final long nodeBGlobalTail = 205;

    StreamAddressSpace s2IdPartial = nodeALogAddressSpace.get(s2Id).copy();
    s2IdPartial.trim(30L);
    s2IdPartial.addAddress(201L);
    s2IdPartial.addAddress(202L);
    s2IdPartial.addAddress(203L);

    Map<UUID, StreamAddressSpace> nodeBLogAddressSpace =
            ImmutableMap.of(s2Id, s2IdPartial, s3Id, getRandomStreamSpace(nodeAGlobalTail - 1));

    when(ctx.getLogUnitClient(nodeA).getLogAddressSpace())
            .thenReturn(
                    getLogAddressSpaceResponse(nodeAGlobalTail, nodeALogAddressSpace, layout.getEpoch()));
    when(ctx.getLogUnitClient(nodeB).getLogAddressSpace())
            .thenReturn(
                    getLogAddressSpaceResponse(nodeBGlobalTail, nodeBLogAddressSpace, layout.getEpoch()));

    StreamsAddressResponse resp = Utils.getLogAddressSpace(ctx.getRuntimeLayout());

    assertThat(resp.getLogTail()).isEqualTo(nodeBGlobalTail);

    Map<UUID, StreamAddressSpace> expectedMergedTails =
            ImmutableMap.of(
                    s1Id, nodeALogAddressSpace.get(s1Id),
                    s2Id, s2IdPartial,
                    s3Id, nodeBLogAddressSpace.get(s3Id));

    assertThat(resp.getAddressMap()).isEqualTo(expectedMergedTails);

    verify(ctx.getLogUnitClient(nodeA), times(1)).getLogAddressSpace();
    verify(ctx.getLogUnitClient(nodeB), times(1)).getLogAddressSpace();
    verify(ctx.getRuntimeLayout(), times(1)).getLogUnitClient(nodeA);
    verify(ctx.getRuntimeLayout(), times(1)).getLogUnitClient(nodeB);
    verify(ctx.getRuntimeLayout(), times(0)).getLogUnitClient(nodeC);

    when(ctx.getLogUnitClient(nodeA).getLogAddressSpace())
            .thenReturn(
                    getLogAddressSpaceResponse(
                            nodeAGlobalTail, nodeALogAddressSpace, layout.getEpoch() + 1));
    assertThatThrownBy(() -> Utils.getLogAddressSpace(ctx.getRuntimeLayout()))
            .isInstanceOf(WrongEpochException.class);
  }
}
