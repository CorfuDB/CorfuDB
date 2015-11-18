/**
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.corfudb.infrastructure;

import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.Setter;
import org.corfudb.infrastructure.wireprotocol.NettyCorfuMsg;
import org.corfudb.infrastructure.wireprotocol.NettyLayoutConfigMsg;
import org.corfudb.runtime.protocols.IServerProtocol;
import org.corfudb.runtime.view.CorfuDBView;

import org.corfudb.runtime.view.ViewJanitor;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.*;

import javax.json.Json;
import javax.json.JsonObject;

/**
 * This class participates in keeping consensus about meta-data.
 *
 * There are two parts, a passive meta-data keeper server, and an active layout-monitor that drives changes to the layout.
 *
 * - A NettyLayoutKeeper server stores the latest committed meta-data.
 *   It participates as a Paxos 'acceptor' in voting on proposals for meta-data changes.
 *
 * - A NettyLayoutKeeper monitor becomes activated upon storing a committed Corfu layout for the first time.
 *   It then instantiates a local CorfuDBView which has connection endpoints to all the layout components.
 *   It spawns a thread that constantly monitors the health of other components.
 *
 *   The monitor may initiate change proposals and may also receive requests from clients to drive changes.
 *   To drive a layout change, the monitor picks a rank and tries to become a Paxos leader and affect the change via a consensus decision.
 */
public class NettyLayoutKeeper extends AbstractNettyServer implements ICorfuDBServer {
    private static Logger log = LoggerFactory.getLogger(NettyLayoutKeeper.class);

    ConsensusKeeper<ConsensusLayoutObject> commitLayout = new ConsensusKeeper(new ConsensusLayoutObject());
    CorfuDBView currentView = null;
    JsonObject newProposal = null;

    public NettyLayoutKeeper() {
    }

    @Override
    void parseConfiguration(Map<String, Object> params) {
        log.info("NettyLayoutKeeper params {}", params);
    }

    @Override
    public void processMessage(NettyCorfuMsg corfuMsg, ChannelHandlerContext ctx) {

        // todo: if not bootstrapped yet, ignore all by bootsrapLayout requests

        log.info("Received request of type {}", corfuMsg.getMsgType());
        NettyLayoutConfigMsg m = (NettyLayoutConfigMsg)corfuMsg;
        switch (corfuMsg.getMsgType())
        {
            case META_PROPOSE_REQ: {
                commitLayout.putHighPhase2Proposal(m.getEpoch(), m.getRank(), m.getJo());

                NettyLayoutConfigMsg resp = new NettyLayoutConfigMsg(
                        NettyCorfuMsg.NettyCorfuMsgType.META_PROPOSE_RES,
                        commitLayout.getEpoch(),
                        commitLayout.getHighPhase1Rank()
                );
                sendResponse(resp, corfuMsg, ctx);
                break;
            }

            case META_COLLECT_REQ: {
                commitLayout.getHighPhase2Proposal(m.getEpoch(), m.getRank());

                NettyLayoutConfigMsg resp =
                    new NettyLayoutConfigMsg(
                            NettyCorfuMsg.NettyCorfuMsgType.META_COLLECT_RES,
                            commitLayout.getEpoch(),
                            commitLayout.getHighPhase2Rank()
                    );

                if (commitLayout.getCommitState() != null)
                    resp.setJo(commitLayout.commitState.getCurrentLayout());
                log.info("layout response {}", resp);
                sendResponse(resp, corfuMsg, ctx);
                break;
            }

            case META_COMMIT: {
                commitLayout.commitProposal(m.getJo()); // TODO this should use 2-step protocol, unless rank == -1 ?
                reconfig(m.getJo());
                break;
            }

            case META_QUERY_REQ: {
                NettyLayoutConfigMsg resp =
                        new NettyLayoutConfigMsg(
                                NettyCorfuMsg.NettyCorfuMsgType.META_QUERY_RES,
                                commitLayout.getEpoch(),
                                commitLayout.getHighPhase2Rank()
                        );

                if (commitLayout.getCommitState() != null)
                    resp.setJo(commitLayout.commitState.getCurrentLayout());
                log.info("layout response {}", resp);
                sendResponse(resp, corfuMsg, ctx);
                break;
            }

            case META_LEADER_REQ: {
                // todo: enforce a delay (300 millisecs?) between leader requests

                synchronized (newProposal) {
                    if (newProposal != null) { // reject; handle leader roles one at a time
                        NettyLayoutConfigMsg resp = new NettyLayoutConfigMsg(NettyCorfuMsg.NettyCorfuMsgType.META_LEADER_RES,
                                commitLayout.getEpoch(),
                                commitLayout.highPhase1Rank
                        );
                        // todo ? sendResponse(resp, corfuMsg, ctx);
                    } else {
                        newProposal = m.getJo();
                        // todo ? monitorThread.interrupt();
                    }
                }
                break;
            }

            default:
                log.warn("unrecognized Layout keeper message type {}", corfuMsg.getMsgType());
                break;
        }
    }


    @Override
    public void reset() {
        log.info("RESET requested, resetting all nodes and incrementing epoch");
        // TODO reset to which configuration??
    }

    public void reconfig(JsonObject newLayout) { // todo make it private!
        // instantiate new CorfuDB view
        if (currentView != null) {
            currentView.invalidate();
        }
        currentView = new CorfuDBView(newLayout);
    }

    public interface ConsensusObject {
        abstract void apply(Object proposal) ;
    }

    @Setter
    @Getter
    public class ConsensusLayoutObject implements ConsensusObject {
        JsonObject currentLayout = null;

        @Override
        public void apply(Object proposal) {
            // TODO check epoch successsion
            setCurrentLayout((JsonObject) proposal);
        }
    }

    @Getter
    public class ConsensusKeeper<T extends ConsensusObject> {

        private long highPhase1Rank;
        long highPhase2Rank;
        Object highPhase2Proposal;
        long epoch;

        T commitState = null;

        ConsensusKeeper(T initialState) { this.commitState = initialState; }

        ConsensusKeeper<T> getHighPhase2Proposal(long epoch, long rank) {
            if (rank > highPhase1Rank)
                highPhase1Rank = rank;
            return this;
        }


        /**
         * @param rank
         * @param proposal
         */
        void putHighPhase2Proposal(long epoch, long rank, Object proposal) {
            if (epoch >= this.epoch && rank >= highPhase1Rank) {
                // accept proposal
                highPhase2Rank = highPhase1Rank = rank;
                highPhase2Proposal = proposal;
                // todo should we learn from the proposal if a higher epoch has been installed by other layout servers already?
            }
        }

        void commitProposal(Object proposal) {
            commitState.apply(proposal);
        }
    }
}
