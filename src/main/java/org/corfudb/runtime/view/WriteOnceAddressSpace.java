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

import org.corfudb.infrastructure.thrift.Hints;
import org.corfudb.runtime.*;
import org.corfudb.runtime.exceptions.*;
import org.corfudb.runtime.protocols.IServerProtocol;
import org.corfudb.runtime.protocols.logunits.IWriteOnceLogUnit;
import org.corfudb.runtime.protocols.replications.IReplicationProtocol;
import org.corfudb.runtime.smr.MultiCommand;
import org.corfudb.runtime.smr.Pair;
import org.corfudb.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.function.Supplier;

/**
 * This view implements a simple write once address space
 *
 * @author Michael Wei <mwei@cs.ucsd.edu>
 */

public class WriteOnceAddressSpace extends CorfuDBRuntimeComponent implements IWriteOnceAddressSpace {

    private final Logger log = LoggerFactory.getLogger(WriteOnceAddressSpace.class);

    public WriteOnceAddressSpace(ICorfuDBInstance corfuInstance)
    {
        super(corfuInstance);
    }

    public void write(long address, Serializable s)
            throws IOException, OverwriteException, TrimmedException, OutOfSpaceException
    {
        try (ByteArrayOutputStream bs = new ByteArrayOutputStream())
        {
            try (ObjectOutput out = new ObjectOutputStream(bs))
            {
                out.writeObject(s);
                write(address, bs.toByteArray());
            }
        }

    }
    public void write(long address, byte[] data)
            throws OverwriteException, TrimmedException, OutOfSpaceException
    {
       /* while (true)
        {
            try {*/
        CorfuDBViewSegment segments =  getView.get().getSegments().get(0);
        IReplicationProtocol replicationProtocol = segments.getReplicationProtocol();

        Set<UUID> streams = null;
        if (logID != null)
            streams = Collections.singleton(logID);

        replicationProtocol.write(corfuInstance, address, streams, data);
        return;
           /* }
            catch (NetworkException e)
            {
                log.warn("Unable to write, requesting new view.", e);
                client.invalidateViewAndWait(e);
                //okay so, if we read on the same address, is it now successful?
                try {
                    if (Arrays.equals(read(address), data)) {
                        return;
                    }
                }
                catch (Exception ex)
                {
                    log.warn("View refreshed, but write was not successful: retrying.", ex);
                }
            }
        }*/
    }

    public byte[] read(long address)
            throws UnwrittenException, TrimmedException
    {
        //TODO: cache the layout so we don't have to determine it on every write.
        CorfuDBViewSegment segments =  getView.get().getSegments().get(0);
        IReplicationProtocol replicationProtocol = segments.getReplicationProtocol();

        if (logID != null)
            return replicationProtocol.read(corfuInstance, address, logID);
        return replicationProtocol.read(corfuInstance, address, null);
    }

    public Object readObject(long address)
            throws UnwrittenException, TrimmedException, ClassNotFoundException, IOException
    {
        byte[] payload = read(address);
        try (ByteArrayInputStream bis = new ByteArrayInputStream(payload))
        {
            try (ObjectInputStream ois = new ObjectInputStream(bis))
            {
                return ois.readObject();
            }
        }
    }

    @Override
    public Hints readHints(long address)
            throws UnwrittenException, TrimmedException
    {
        //TODO: cache the layout so we don't have to determine it on every write.
        try {
            CorfuDBViewSegment segments =  getView.get().getSegments().get(0);
            IReplicationProtocol replicationProtocol = segments.getReplicationProtocol();

            return replicationProtocol.readHints(address);
        }
        catch (NetworkException e)
        {
            log.warn("Unable to set hint, you might want to reconfig LUs? {}", e);
            return null;
        }
    }

    @Override
    public void setHintsNext(long address, UUID stream, long nextOffset)
            throws UnwrittenException, TrimmedException
    {
        //TODO: cache the layout so we don't have to determine it on every write.
        try {
            CorfuDBViewSegment segments =  getView.get().getSegments().get(0);
            IReplicationProtocol replicationProtocol = segments.getReplicationProtocol();
            replicationProtocol.setHintsNext(address, stream, nextOffset);
            return;
        }
        catch (NetworkException e)
        {
            log.warn("Unable to set hint, you might want to reconfig LUs? {}", e);
            return;
        }
    }

    @Override
    public void setHintsTxDec(long address, boolean dec)
            throws UnwrittenException, TrimmedException
    {
        //TODO: cache the layout so we don't have to determine it on every write.
        try {
            CorfuDBViewSegment segments =  getView.get().getSegments().get(0);
            IReplicationProtocol replicationProtocol = segments.getReplicationProtocol();
            replicationProtocol.setHintsTxDec(address, dec);
            return;
        }
        catch (NetworkException e)
        {
            log.warn("Unable to set hint, you might want to reconfig LUs? {}", e);
            return;
        }
    }

    @Override
    public void setHintsFlatTxn(long address, MultiCommand flatTxn) throws UnwrittenException, TrimmedException, IOException {
        try (ByteArrayOutputStream bs = new ByteArrayOutputStream())
        {
            try (ObjectOutput out = new ObjectOutputStream(bs))
            {
                out.writeObject(flatTxn);

                try {
                    CorfuDBViewSegment segments =  getView.get().getSegments().get(0);
                    IReplicationProtocol replicationProtocol = segments.getReplicationProtocol();
                    replicationProtocol.setHintsFlatTxn(address, flatTxn);
                    return;
                }
                catch (NetworkException e)
                {
                    log.warn("Unable to set hint, you might want to reconfig LUs? {}", e);
                    return;
                }
            }
        }
    }
}


