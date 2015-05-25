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
package org.corfudb.runtime.entries.legacy;

import org.corfudb.runtime.entries.IStreamEntry;
import org.corfudb.runtime.stream.*;
import java.util.*;

public class HopAdapterStreamEntry implements IAdapterStreamEntry
{
    IStreamEntry cde;
    public HopAdapterStreamEntry(IStreamEntry tcde)
    {
        cde = tcde;
    }

    @Override
    public ITimestamp getTimestamp()
    {
        return cde.getTimestamp();
    }

    public List<UUID> getStreamIds()
    {
        return cde.getStreamIds();
    }

    public List<Long> getIntegerStreamIds()
    {
        ArrayList<Long> result = new ArrayList();
        for(UUID uuid : cde.getStreamIds()) {
            result.add(uuid.getMostSignificantBits());
        }
        return result;
    }

    /**
     * Set the timestamp.
     * @param   ts  The timestamp of this entry.
     */
    public void setTimestamp(ITimestamp ts) { cde.setTimestamp(ts); }

    @Override
    public Object getPayload()
    {
        return cde.getPayload();
    }

    @Override
    public boolean containsStream(UUID streamid)
    {
        return cde.containsStream(streamid);
    }

    @Override
    public boolean containsStream(long streamid)
    {
        return cde.containsStream(new UUID(streamid,0));
    }
}

