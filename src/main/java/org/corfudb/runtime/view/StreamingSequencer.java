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

package org.corfudb.runtime.view;

import org.corfudb.runtime.CorfuDBRuntime;
import org.corfudb.runtime.protocols.IServerProtocol;
import org.corfudb.runtime.protocols.sequencers.ISimpleSequencer;
import org.corfudb.runtime.protocols.sequencers.IStreamSequencer;
import org.corfudb.runtime.RemoteException;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

/**
 * This view implements a streaming sequencer. It tries to use a streaming sequencer,
 * if available, but falls back to a non-stream aware sequencer if it is not.
 *
 * @author Michael Wei <mwei@cs.ucsd.edu>
 */

public class StreamingSequencer {

    private CorfuDBRuntime client;
    private UUID logID;
    private CorfuDBView view;
    private Supplier<CorfuDBView> getView;

    private final Logger log = LoggerFactory.getLogger(StreamingSequencer.class);

    public StreamingSequencer(CorfuDBRuntime client)
    {
        this.client = client;
        this.getView = this.client::getView;
    }

    public StreamingSequencer(CorfuDBRuntime client, UUID logID)
    {
        this.client = client;
        this.logID = logID;
        this.getView = () -> {
            try {
            return this.client.getView(this.logID);
            }
            catch (RemoteException re)
            {
                log.warn("Error getting remote view", re);
                return null;
            }
        };
    }

    public StreamingSequencer(CorfuDBView view)
    {
        this.view = view;
        this.getView = () -> {
            return this.view;
        };
    }

    public long getNext(UUID streamID)
    {
        return getNext(streamID, 1);
    }

    public long getNext(UUID streamID, int numTokens)
    {
        while (true)
        {
            try {
                IServerProtocol sequencer= getView.get().getSequencers().get(0);
                if (sequencer instanceof IStreamSequencer)
                {
                    return ((IStreamSequencer)sequencer).sequenceGetNext(streamID, numTokens);
                }
                else
                {
                    return ((ISimpleSequencer)sequencer).sequenceGetNext();
                }
            }
            catch (Exception e)
            {
                log.warn("Unable to get next sequence, requesting new view.", e);
                client.invalidateViewAndWait();
            }
        }
    }

    public long getCurrent(UUID streamID)
    {
        while (true)
        {
            try {
                IServerProtocol sequencer = getView.get().getSequencers().get(0);
                if (sequencer instanceof IStreamSequencer)
                {
                    return ((IStreamSequencer)sequencer).sequenceGetCurrent(streamID);
                }
                else
                {
                    return ((ISimpleSequencer)sequencer).sequenceGetCurrent();
                }
            }
            catch (Exception e)
            {
                log.warn("Unable to get current sequence, requesting new view.", e);
                client.invalidateViewAndWait();
            }
        }
    }

    public void setAllocationSize(UUID streamID, int size)
    {
        while (true)
        {
            try {
                IServerProtocol sequencer = getView.get().getSequencers().get(0);
                if (sequencer instanceof IStreamSequencer)
                {
                    ((IStreamSequencer)sequencer).setAllocationSize(streamID, size);
                    return;
                }
                else
                {
                    return;
                }
            }
            catch (Exception e)
            {
                log.warn("Unable to get current sequencer, requesting new view.", e);
                client.invalidateViewAndWait();
            }
        }

    }

}


