package org.corfudb.protocols.service;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestPayloadMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponsePayloadMsg;
import org.corfudb.runtime.proto.service.Layout.BootstrapLayoutRequestMsg;
import org.corfudb.runtime.proto.service.Layout.BootstrapLayoutResponseMsg;
import org.corfudb.runtime.proto.service.Layout.CommitLayoutRequestMsg;
import org.corfudb.runtime.proto.service.Layout.CommitLayoutResponseMsg;
import org.corfudb.runtime.proto.service.Layout.LayoutRequestMsg;
import org.corfudb.runtime.proto.service.Layout.LayoutResponseMsg;
import org.corfudb.runtime.proto.service.Layout.PrepareLayoutRequestMsg;
import org.corfudb.runtime.proto.service.Layout.PrepareLayoutResponseMsg;
import org.corfudb.runtime.proto.service.Layout.ProposeLayoutRequestMsg;
import org.corfudb.runtime.proto.service.Layout.ProposeLayoutResponseMsg;
import org.corfudb.runtime.view.Layout;

import static org.corfudb.protocols.CorfuProtocolCommon.getLayoutMsg;

/**
 * This class provides methods for creating the Protobuf objects defined
 * in layout.proto. These provide the interface for performing the RPCs
 * handled by the Layout server.
 */
@Slf4j
public final class CorfuProtocolLayout {
    // Prevent class from being instantiated
    private CorfuProtocolLayout() {}

    /**
     * Returns a layout request message that can be sent by the client.
     *
     * @param epoch the epoch of requested Layout.
     * @return  a RequestPayloadMsg containing a layout request.
     */
    public static RequestPayloadMsg getLayoutRequestMsg(long epoch) {
        return RequestPayloadMsg.newBuilder()
                .setLayoutRequest(LayoutRequestMsg.newBuilder()
                        .setEpoch(epoch)
                        .build())
                .build();
    }

    /**
     * Returns a layout response message that can be sent by the server.
     *
     * @param layout the Layout object to be sent by the server.
     * @return a ResponsePayloadMsg containing a layout response.
     */
    public static ResponsePayloadMsg getLayoutResponseMsg(Layout layout) {
        return ResponsePayloadMsg.newBuilder()
                .setLayoutResponse(LayoutResponseMsg.newBuilder()
                        .setLayout(getLayoutMsg(layout))
                        .build())
                .build();
    }

    /**
     * Returns a prepare layout request message that can be sent by the client.
     *
     * @param epoch epoch for which the paxos rounds are being run.
     * @param rank  The rank to use for the prepare.
     * @return a RequestPayloadMsg containing a prepare layout request.
     */
    public static RequestPayloadMsg getPrepareLayoutRequestMsg(long epoch, long rank) {
        return RequestPayloadMsg.newBuilder()
                .setPrepareLayoutRequest(PrepareLayoutRequestMsg.newBuilder()
                        .setEpoch(epoch)
                        .setRank(rank)
                        .build())
                .build();
    }

    /**
     * Returns a prepare layout response message that can be sent by the client.
     *
     * @param prepared boolean value indicates whether the prepare was successful or not.
     * @param rank     the rank used for the prepare.
     * @param layout   the prepared layout if the prepare request was successful.
     * @return a ResponsePayloadMsg containing a prepare layout response.
     */
    public static ResponsePayloadMsg getPrepareLayoutResponseMsg(boolean prepared,
                                                                 long rank, Layout layout) {
        return ResponsePayloadMsg.newBuilder()
                .setPrepareLayoutResponse(PrepareLayoutResponseMsg.newBuilder()
                        .setPrepared(prepared)
                        .setRank(rank)
                        .setLayout(getLayoutMsg(layout))
                        .build())
                .build();
    }

  /**
   * Returns a propose layout request message that can be sent by the client.
   *
   * @param epoch  epoch for which the paxos rounds are being run
   * @param rank   The rank to use for the propose. It should be the same
   *               rank from a successful prepare (phase 1).
   * @param layout The layout to install for phase 2.
   * @return a RequestPayloadMsg containing a propose layout request.
   */
  public static RequestPayloadMsg getProposeLayoutRequestMsg(long epoch, long rank, Layout layout) {
        return RequestPayloadMsg.newBuilder()
                .setProposeLayoutRequest(ProposeLayoutRequestMsg.newBuilder()
                        .setEpoch(epoch)
                        .setRank(rank)
                        .setLayout(getLayoutMsg(layout))
                        .build())
                .build();
    }

    /**
     * Returns a propose layout response message that can be sent by the client.
     *
     * @param proposed boolean value indicates whether the propose was successful or not.
     * @param rank     used for initializing OutrankedException upon REJECT.
     * @return a ResponsePayloadMsg containing a propose layout response.
     */
    public static ResponsePayloadMsg getProposeLayoutResponseMsg(boolean proposed, long rank) {
        return ResponsePayloadMsg.newBuilder()
                .setProposeLayoutResponse(ProposeLayoutResponseMsg.newBuilder()
                        .setProposed(proposed)
                        .setRank(rank)
                        .build())
                .build();
    }

    /**
     * Returns a commit layout request message that can be sent by the client.
     *
     * @param forced boolean field indicates whether to force the commit or not.
     * @param epoch  epoch affiliated with the layout.
     * @param layout Layout to be committed.
     * @return a RequestPayloadMsg containing a commit layout request.
     */
    public static RequestPayloadMsg getCommitLayoutRequestMsg(boolean forced, long epoch, Layout layout) {
        return RequestPayloadMsg.newBuilder()
                .setCommitLayoutRequest(CommitLayoutRequestMsg.newBuilder()
                        .setForced(forced)
                        .setEpoch(epoch)
                        .setLayout(getLayoutMsg(layout))
                        .build())
                .build();
    }

    /**
     * Returns a commit layout response message that can be sent by the client.
     *
     * @param committed boolean value indicates whether the commit was successful or not.
     * @return a ResponsePayloadMsg containing a commit layout response.
     */
    public static ResponsePayloadMsg getCommitLayoutResponseMsg(boolean committed) {
        return ResponsePayloadMsg.newBuilder()
                .setCommitLayoutResponse(CommitLayoutResponseMsg.newBuilder()
                        .setCommitted(committed)
                        .build())
                .build();
    }

    /**
     * Returns a bootstrap layout request message that can be sent by the client.
     *
     * @param layout the layout to bootstrap with.
     * @return a RequestPayloadMsg containing a bootstrap layout request.
     */
    public static RequestPayloadMsg getBootstrapLayoutRequestMsg(Layout layout) {
        return RequestPayloadMsg.newBuilder()
                .setBootstrapLayoutRequest(BootstrapLayoutRequestMsg.newBuilder()
                        .setLayout(getLayoutMsg(layout))
                        .build())
                .build();
    }

    /**
     * Returns a bootstrap layout response message that can be sent by the client.
     *
     * @param bootstrapped an enum value indicates whether the commit was successful or not.
     * @return a ResponsePayloadMsg containing a bootstrap layout response.
     */
    public static ResponsePayloadMsg getBootstrapLayoutResponseMsg(boolean bootstrapped) {
        return ResponsePayloadMsg.newBuilder()
                .setBootstrapLayoutResponse(BootstrapLayoutResponseMsg.newBuilder()
                        .setBootstrapped(bootstrapped)
                        .build())
                .build();
    }
}
