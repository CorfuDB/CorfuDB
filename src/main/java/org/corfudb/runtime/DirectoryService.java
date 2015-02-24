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

package org.corfudb.runtime;

import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CyclicBarrier;

import gnu.getopt.Getopt;
import org.corfudb.runtime.collections.CorfuDBMap;
import org.corfudb.sharedlog.ClientLib;
import org.corfudb.sharedlog.CorfuException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.impl.SimpleLogger;
import org.corfudb.runtime.collections.CorfuDBCounter;
import org.corfudb.runtime.collections.CorfuDBCoarseList;
import org.corfudb.runtime.collections.CorfuDBList;



/**
 * This is a directory service that maps from human-readable names to CorfuDB object IDs.
 * It's built using CorfuDB objects that run over hardcoded IDs (MAX_LONG and MAX_LONG-1).
 */
public class DirectoryService
{
    AbstractRuntime TR;
    CorfuDBMap<String, Long> names;
    CorfuDBCounter idctr;

    static long DS_RESERVED_MAP_ID = 0;
    static long DS_RESERVED_CTR_ID = 1;
    static long DS_RESERVED_UNIQUE_ID = 2;
    static long FIRST_VALID_STREAM_ID = 3;

    public DirectoryService(AbstractRuntime tTR)
    {
        TR = tTR;
        names = new CorfuDBMap(TR, DS_RESERVED_MAP_ID);
        idctr = new CorfuDBCounter(TR, DS_RESERVED_CTR_ID);

    }

    /**
     * Returns a unique ID. This ID is guaranteed to be unique
     * system-wide with respect to other IDs generated across the system
     * by the getUniqueID call parameterized with a streamfactory running over
     * the same log address space. It's implemented by appending an entry
     * to the underlying log and returning the timestamp/position.
     *
     * Note: it is not guaranteed to be unique with respect to IDs returned
     * by nameToStreamID.
     *
     * @param sf StreamFactory to use
     * @return system-wide unique ID
     */
    public static long getUniqueID(StreamFactory sf)
    {
        Stream S = sf.newStream(DS_RESERVED_UNIQUE_ID);
        HashSet hs = new HashSet(); hs.add(DS_RESERVED_UNIQUE_ID);
        return (Long)S.append("DummyString", hs); //todo: remove the cast
    }


    /**
     * Maps human-readable name to object ID. If no such human-readable name exists already,
     * a new mapping is created.
     *
     * @param X
     * @return
     */
    public long nameToStreamID(String X)
    {
        System.out.println("Mapping " + X);
        long ret;
        while(true)
        {
            TR.BeginTX();
            if (names.containsKey(X))
                ret = names.get(X);
            else
            {
                ret = idctr.read() + FIRST_VALID_STREAM_ID;
                idctr.increment();
                names.put(X, ret);
            }
            if(TR.EndTX()) break;
        }
        System.out.println("Mapped " + X + " to " + ret);
        return ret;
    }

}

