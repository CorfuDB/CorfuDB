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
import org.corfudb.infrastructure.configmaster.policies.IReconfigurationPolicy;
import org.corfudb.infrastructure.configmaster.policies.SimpleReconfigurationPolicy;
import org.corfudb.infrastructure.wireprotocol.NettyCorfuMsg;
import org.corfudb.infrastructure.wireprotocol.NettyCollectResponseMsg;
import org.corfudb.infrastructure.wireprotocol.NettyProposeRequestMsg;
import org.corfudb.infrastructure.wireprotocol.NettyProposeResponseMsg;
import org.corfudb.runtime.NetworkException;
import org.corfudb.runtime.view.CorfuDBView;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.io.*;
import java.util.*;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;

/*
import com.esotericsoftware.kryonet.Server;
import com.esotericsoftware.kryonet.Listener;
import com.esotericsoftware.kryonet.Connection;
*/

import org.corfudb.runtime.view.Serializer;

/**
 * This class participates in keeping consensus about meta-data.
 *
 * There are two parts, a passive meta-data keeper server, and an active layout-monitor that drives changes to the layout.
 *
 * - A NettyMetaDataKeeper server stores the latest committed meta-data.
 *   It participates as a Paxos 'acceptor' in voting on proposals for meta-data changes.
 *
 * - A NettyMetaDataKeeper monitor becomes activated upon storing a committed Corfu layout for the first time.
 *   It then instantiates a local CorfuDBView which has connection endpoints to all the layout components.
 *   It spawns a thread that constantly monitors the health of other components.
 *
 *   The monitor may initiate change proposals and may also receive requests from clients to drive changes.
 *   To drive a layout change, the monitor picks a rank and tries to become a Paxos leader and affect the change via a consensus decision.
 */
public class NettyMetaDataKeeper extends AbstractNettyServer implements ICorfuDBServer {
    private static Logger log = LoggerFactory.getLogger(NettyMetaDataKeeper.class);

    ConsensusKeeper currentConfig = new ConsensusKeeper(new ConsensusLayoutObject());
    CorfuDBView currentView = null;

    public NettyMetaDataKeeper() {
    }

    @Override
    void parseConfiguration(Map<String, Object> configuration) {
        log.info("NettyMetaDataKeeper configuration {}", configuration);
    }

    @Override
    public void processMessage(NettyCorfuMsg corfuMsg, ChannelHandlerContext ctx) {

        log.info("Received request of type {}", corfuMsg.getMsgType());
        switch (corfuMsg.getMsgType())
        {
            case META_PROPOSE_REQ: {
                NettyProposeRequestMsg m = (NettyProposeRequestMsg)corfuMsg;

                currentConfig.commitProposal(m.getJo()); // TODO this should use propose, eventually, unless rank == -1 ?
                reconfig(m.getJo());

                NettyProposeResponseMsg resp = new NettyProposeResponseMsg(true);
                sendResponse(resp, corfuMsg, ctx);
                break;
            }

            case META_COLLECT_REQ: {
                NettyCollectResponseMsg resp = new NettyCollectResponseMsg((JsonObject)( (ConsensusLayoutObject) currentConfig.getState() ).getCurrentLayout() );
                log.info("layout response {}", resp);
                sendResponse(resp, corfuMsg, ctx);
                break;
            }

            default:
                break;
        }
    }


    @Override
    public void reset() {
        log.info("RESET requested, resetting all nodes and incrementing epoch");
        // TODO reset to which configuration??
    }

    private void reconfig(JsonObject newLayout) {
        // instantiate new CorfuDB view
        if (currentView != null) {
            currentView.invalidate();
        }
        currentView = new CorfuDBView(newLayout);

        // TODO the next part should be done by proposer or consensus engine?
        UUID logID =  UUID.randomUUID();
        log.info("New log instance id= " + logID.toString());
        currentView.setUUID(logID);

        // TODO if no monitor thread exists already, start a monitor thread

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
    public class ConsensusKeeper {

        private int highPhase1Rank;
        private int highPhase2Rank;
        private Object highPhase2Proposal;
        private int epoch;

        ConsensusObject state = null;

        ConsensusKeeper(ConsensusObject initialState) { this.state = initialState; }

        Object getHighPhase2Proposal(int rank) {
            if (rank > highPhase1Rank) {
                highPhase1Rank = rank;
                return highPhase2Proposal;
            } else {
                return null;
            }
        }


        /**
         * @param rank
         * @param proposal
         * @return 0 means proposal accepted, -1 means it is rejected
         */
        int putHighPhase2Proposal(int epoch, int rank, Object proposal) {
            if (rank >= highPhase1Rank) {
                // accept proposal
                highPhase2Rank = highPhase1Rank = rank;
                highPhase2Proposal = proposal;
                // todo should we learn from the proposal if a higher epoch has been installed by other layout servers already?
                return 0;
            } else {
                return -1;
            }
        }

        void commitProposal(Object proposal) {
            state.apply(proposal);
        }
    }
}
