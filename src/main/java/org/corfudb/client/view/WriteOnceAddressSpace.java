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
import org.corfudb.client.CorfuDBViewSegment;
import org.corfudb.client.IServerProtocol;
import org.corfudb.client.logunits.IWriteOnceLogUnit;
import org.corfudb.client.UnwrittenException;
import org.corfudb.client.TrimmedException;
import org.corfudb.client.OverwriteException;
import org.corfudb.client.NetworkException;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This view implements a simple write once address space
 *
 * @author Michael Wei <mwei@cs.ucsd.edu>
 */

public class WriteOnceAddressSpace {

    private CorfuDBClient client;
	private final Logger log = LoggerFactory.getLogger(WriteOnceAddressSpace.class);

    public WriteOnceAddressSpace(CorfuDBClient client)
    {
        this.client = client;
    }

    public void write(long address, byte[] data)
        throws OverwriteException, TrimmedException
    {
        while (true)
        {
            try {
                //TODO: handle multiple segments
                CorfuDBViewSegment segments =  client.getView().getSegments().get(0);
                int mod = segments.getGroups().size();
                int groupnum =(int) (address % mod);
                List<IServerProtocol> chain = segments.getGroups().get(groupnum);
                //writes have to go to chain in order
                long mappedAddress = address/mod;
                for (IServerProtocol unit : chain)
                {
                    ((IWriteOnceLogUnit)unit).write(address,data);
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
                //TODO: handle multiple segments
                CorfuDBViewSegment segments =  client.getView().getSegments().get(0);
                int mod = segments.getGroups().size();
                int groupnum =(int) (address % mod);
                List<IServerProtocol> chain = segments.getGroups().get(groupnum);
                //reads have to come from last unit in chain
                IWriteOnceLogUnit wolu = (IWriteOnceLogUnit) chain.get(chain.size() - 1);
                long mappedAddress = address/mod;
                return wolu.read(mappedAddress);
            }
            catch (NetworkException e)
            {
                log.warn("Unable to read, requesting new view.", e);
                client.invalidateViewAndWait();
            }
        }
    }
}


