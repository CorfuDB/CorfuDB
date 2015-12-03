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

import org.corfudb.runtime.*;
import org.corfudb.runtime.protocols.IServerProtocol;
import org.corfudb.runtime.protocols.logunits.IWriteOnceLogUnit;

import java.util.Collections;
import java.util.List;

import org.corfudb.runtime.protocols.replications.IReplicationProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.io.IOException;

import java.util.Set;
import java.util.function.Supplier;

import java.util.UUID;
/**
 * This view implements a cached write once address space
 *
 * @author Michael Wei <mwei@cs.ucsd.edu>
 */

public class CachedWriteOnceAddressSpace extends CorfuDBRuntimeComponent implements IWriteOnceAddressSpace {
    private final Logger log = LoggerFactory.getLogger(CachedWriteOnceAddressSpace.class);

    public CachedWriteOnceAddressSpace(ICorfuDBInstance corfuInstance)
    {
        super(corfuInstance);
    }

    public void write(long address, Serializable s)
            throws IOException, OverwriteException, TrimmedException, OutOfSpaceException
    {
        write(address, Serializer.serialize_compressed(s));
    }

    public void write(long address, byte[] data)
            throws OverwriteException, TrimmedException, OutOfSpaceException
    {
        //TODO: handle multiple segments
        CorfuDBViewSegment segments =  getView.get().getSegments().get(0);
        IReplicationProtocol replicationProtocol = segments.getReplicationProtocol();
        replicationProtocol.write(corfuInstance, address, Collections.singleton(getView.get().getLogID()), data);
        return;

    }

    public byte[] read(long address)
            throws UnwrittenException, TrimmedException
    {
        //TODO: cache the layout so we don't have to determine it on every write.

        byte[] data = null;
        data = AddressSpaceCache.get(getView.get().getLogID(), address);
        if (data != null) {
            return data;
        }

        //TODO: handle multiple segments

        CorfuDBViewSegment segments =  getView.get().getSegments().get(0);
        IReplicationProtocol replicationProtocol = segments.getReplicationProtocol();
        data = replicationProtocol.read(corfuInstance, address, getView.get().getLogID());
        AddressSpaceCache.put(getView.get().getLogID(), address, data);
        return data;
    }

    public Object readObject(long address)
            throws UnwrittenException, TrimmedException, ClassNotFoundException, IOException
    {
        byte[] data = read(address);
        return Serializer.deserialize_compressed(data);
    }
}


