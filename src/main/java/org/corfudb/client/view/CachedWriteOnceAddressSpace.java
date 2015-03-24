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

package org.corfudb.client.view;

import org.corfudb.client.CorfuDBClient;
import org.corfudb.client.CorfuDBView;
import org.corfudb.client.CorfuDBViewSegment;
import org.corfudb.client.IServerProtocol;
import org.corfudb.client.logunits.IWriteOnceLogUnit;
import org.corfudb.client.UnwrittenException;
import org.corfudb.client.TrimmedException;
import org.corfudb.client.OverwriteException;
import org.corfudb.client.NetworkException;
import org.corfudb.client.RemoteException;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectOutput;
import java.io.IOException;

import java.util.function.Supplier;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.io.Input;

import java.util.zip.DeflaterOutputStream;
import java.util.zip.DeflaterInputStream;
import java.util.zip.InflaterInputStream;

import java.util.UUID;
/**
 * This view implements a cached write once address space
 *
 * @author Michael Wei <mwei@cs.ucsd.edu>
 */

public class CachedWriteOnceAddressSpace implements IWriteOnceAddressSpace {

    private CorfuDBClient client;
    private UUID logID;
    private CorfuDBView view;
    private Supplier<CorfuDBView> getView;
    private ThreadLocal<Kryo> kryos = new ThreadLocal<Kryo>() {
        protected Kryo initialValue() {
            Kryo kryo = new Kryo();
            SerializerRegistration.registerSerializer(kryo);
            return kryo;
        };
    };

	private final Logger log = LoggerFactory.getLogger(CachedWriteOnceAddressSpace.class);

    public CachedWriteOnceAddressSpace(CorfuDBClient client)
    {
        this.client = client;
        this.getView = () ->  {
            return this.client.getView();
        };
        this.logID = getView.get().getUUID();
    }

    public CachedWriteOnceAddressSpace(CorfuDBClient client, UUID logID)
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

    public CachedWriteOnceAddressSpace(CorfuDBView view)
    {
        this.view = view;
        this.getView = () -> {
            return this.view;
        };
        this.logID = getView.get().getUUID();
    }

    public void write(long address, Serializable s)
        throws IOException, OverwriteException, TrimmedException
    {
        try (ByteArrayOutputStream bs = new ByteArrayOutputStream())
        {
            try (DeflaterOutputStream dos = new DeflaterOutputStream(bs))
            {
                Kryo k = kryos.get();
                Output o = new Output(dos, 16384);
                k.writeClassAndObject(o, s);
                o.flush();
                o.close();
                dos.flush();
                dos.finish();
                write(address, bs.toByteArray());
            }
        }
    }

    public void write(long address, byte[] data)
        throws OverwriteException, TrimmedException
    {
        while (true)
        {
            try {
                //TODO: handle multiple segments
                CorfuDBViewSegment segments =  getView.get().getSegments().get(0);
                int mod = segments.getGroups().size();
                int groupnum =(int) (address % mod);
                List<IServerProtocol> chain = segments.getGroups().get(groupnum);
                //writes have to go to chain in order
                long mappedAddress = address/mod;
                for (IServerProtocol unit : chain)
                {
                    ((IWriteOnceLogUnit)unit).write(mappedAddress,data);
                    return;
                }
            }
            catch (NetworkException e)
            {
                log.warn("Unable to write, requesting new view.", e);
                client.invalidateViewAndWait();
            }
        }
    }

    public byte[] read(long address)
        throws UnwrittenException, TrimmedException
    {
        //TODO: cache the layout so we don't have to determine it on every write.

        while (true)
        {
            try {
                byte[] data = null;
                data = AddressSpaceCache.get(logID, address);
                if (data != null) {
                    log.debug("Cache hit @ {}", address);
                    return data;
                }

                //TODO: handle multiple segments
                CorfuDBViewSegment segments =  getView.get().getSegments().get(0);
                int mod = segments.getGroups().size();
                int groupnum =(int) (address % mod);
                long mappedAddress = address/mod;

                List<IServerProtocol> chain = segments.getGroups().get(groupnum);
                //reads have to come from last unit in chain
                IWriteOnceLogUnit wolu = (IWriteOnceLogUnit) chain.get(chain.size() - 1);
                data = wolu.read(mappedAddress);
                log.debug("cache MISS @ {}", address);
                AddressSpaceCache.put(logID, address, data);
                return data;
            }
            catch (NetworkException e)
            {
                log.warn("Unable to read, requesting new view.", e);
                client.invalidateViewAndWait();
            }
        }
    }

    public Object readObject(long address)
        throws UnwrittenException, TrimmedException, ClassNotFoundException, IOException
    {
         Kryo k = kryos.get();
         byte[] data = read(address);
         try (ByteArrayInputStream bais = new ByteArrayInputStream(data))
         {
            try (InflaterInputStream dis = new InflaterInputStream(bais))
            {
                try (Input input = new Input(dis, 16384))
                {
                    return k.readClassAndObject(input);
                }
            }
         }
    }
}


