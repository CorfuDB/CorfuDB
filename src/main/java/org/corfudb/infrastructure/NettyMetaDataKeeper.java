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

public class NettyMetaDataKeeper extends AbstractNettyServer implements ICorfuDBServer {
    private static Logger log = LoggerFactory.getLogger(NettyMetaDataKeeper.class);

    private JsonObject currentConfig = null;

    public NettyMetaDataKeeper() {
    }

    @Override
    void parseConfiguration(Map<String, Object> configuration) {
        log.info("NettyMetaDataKeeper configuration {}", configuration);

        // TODO the next part should be part of ClientLocalViewManager
        UUID logID =  UUID.randomUUID();
        log.info("New log instance id= " + logID.toString());
        CorfuDBView currentView = new CorfuDBView(configuration);
        currentView.setUUID(logID);
        currentConfig = currentView.getSerializedJSONView();
    }

    @Override
    public void processMessage(NettyCorfuMsg corfuMsg, ChannelHandlerContext ctx) {

        log.info("Received request of type {}", corfuMsg.getMsgType());
        switch (corfuMsg.getMsgType())
        {
            case META_PROPOSE_REQ: {
                currentConfig = ((NettyProposeRequestMsg)corfuMsg).getJo();
                NettyProposeResponseMsg resp = new NettyProposeResponseMsg(true);
                sendResponse(resp, corfuMsg, ctx);
                break;
            }

            case META_COLLECT_REQ: {
                NettyCollectResponseMsg resp = new NettyCollectResponseMsg(currentConfig);
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

    public class LayoutConsensus {

        private int highPhase1Rank;
        private int highPhase2Rank;
        private Proposal highPhase2Proposal;

        int thisEpoch;

        @Getter
        @Setter
        class Proposal {
            int nextEpoch;
            int rank;
            // currentView
            // nextView
        }

        Proposal getHighPhase2Proposal(int rank) {
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
        int putHighPhase2Proposal(int rank, Proposal proposal) {
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
    /*
        void proposeChange(view newView) {
            Proposal myProposal = new Proposal();
            myProposal.setRank(highPhase1Rank+1);
            int adoptRank = -1;
            boolean isLeader = true;
            boolean isDone = false;

            while (! done) {
                for (ls : layoutServers) {
                    CompletableFuture f = ls.getHighPhase2Proposal(myProposal.getRank());
                    // TODO use completion to increment a completion counter, and to collect responses or process
                }

                // todo wait for counter to reach quorum size
                //

                // obtain the results from the future
                //
                Proposal readProposal = lst.get().result;

                if (readProposal == null) {
                        newRank++;
                        break;
                    }

                    // check that myProposal epoch is highest than last installed
                    //
                    if (myProposal.epoch < readProposal.epoch) {
                        // TODO something
                    }

                    if (highPhase2Proposal.next != null && highPhase2Proposal.nextRank > adoptRank) {
                        adoptRank = highPhase2Rank.nextRank;
                        myProposal = readProposal;
                    }
                }
        }
        */
    }
}
