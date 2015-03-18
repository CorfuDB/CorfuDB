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

package org.corfudb.client;

import org.corfudb.client.view.Sequencer;
import org.corfudb.client.view.WriteOnceAddressSpace;
import org.corfudb.client.CorfuDBClient;
import org.corfudb.client.OutOfSpaceException;

import java.util.Map;
import java.util.ArrayList;
import java.util.Queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.UUID;

public class Timestamp implements Comparable<Timestamp>, Serializable {

    public long epoch;
    public transient Long pos;
    public transient Long physicalPos;
    public transient UUID logID;
    public transient UUID streamID;

    public static final long serialVersionUID = 0l;
    public Timestamp(long epoch, long pos, long physicalPos)
    {
        this.epoch = epoch;
        this.pos = pos;
        this.physicalPos = physicalPos;
    }

    public Timestamp(long epoch)
    {
        this.epoch = epoch;
        this.pos = null;
        this.physicalPos = null;
    }

    public void setEpoch(long newEpoch)
    {
        this.epoch = newEpoch;
    }

    public long getEpoch()
    {
        return epoch;
    }

    @Override
    public String toString()
    {
        if (physicalPos == null)
        {
            return epoch + ".?";
        }
        return epoch + "." + physicalPos;
    }

    public int compareTo(Timestamp t)
    {
        if (logID.equals(t.physicalPos)) {
            return (int)(physicalPos - t.physicalPos);
        }
        if (t.epoch != this.epoch) { return (int) (this.epoch - t.epoch); }
        return (int) (this.pos - t.pos);
    }
}

