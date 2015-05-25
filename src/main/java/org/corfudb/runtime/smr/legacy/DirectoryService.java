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

package org.corfudb.runtime.smr.legacy;

import java.util.*;

import org.corfudb.runtime.collections.CorfuDBMap;
import org.corfudb.runtime.collections.CorfuDBCounter;
import org.corfudb.runtime.smr.IStreamFactory;
import org.corfudb.runtime.stream.IStream;

/**
 * This is a directory service that maps from human-readable names to CorfuDB object IDs.
 * It's built using CorfuDB objects that run over hardcoded IDs (MAX_LONG and MAX_LONG-1).
 */
public class DirectoryService
{
    AbstractRuntime TR;
    CorfuDBMap<String, UUID> names;
    CorfuDBCounter idctr;

    static UUID DS_RESERVED_MAP_ID = new UUID(0, 0);
    static UUID DS_RESERVED_CTR_ID = new UUID(0, 1);
    static long DS_RESERVED_UNIQUE_ID = 2;
    static long FIRST_VALID_STREAM_ID = 3;

    public DirectoryService(AbstractRuntime tTR)
    {
        TR = tTR;
        System.out.println("Object ID for Object-to-Name Map = " + DS_RESERVED_MAP_ID);
        names = new CorfuDBMap(TR, DS_RESERVED_MAP_ID);
        System.out.println("Object ID for Object-to-Name Counter = " + DS_RESERVED_CTR_ID);
        idctr = new CorfuDBCounter(TR, DS_RESERVED_CTR_ID);

    }

    /**
     * Returns a unique ID. This ID is guaranteed to be unique
     * system-wide with respect to other IDs generated across the system
     * by the getUniqueID call parameterized with a streamfactory running over
     * the same stream address space. It's implemented by appending an entry
     * to the underlying stream and returning the timestamp/position.
     *
     * Note: it is not guaranteed to be unique with respect to IDs returned
     * by nameToStreamID.
     *
     * @param sf IStreamFactory to use
     * @return system-wide unique ID
     */
    public static UUID getUniqueID(IStreamFactory sf)
    {
        // IStream S = sf.newStream(DS_RESERVED_UNIQUE_ID);
        // HashSet hs = new HashSet(); hs.add(DS_RESERVED_UNIQUE_ID);
        return UUID.randomUUID();
    }


    /**
     * Maps human-readable name to object ID. If no such human-readable name exists already,
     * a new mapping is created.
     *
     * @param X
     * @return
     */
    public UUID nameToStreamID(String X)
    {
        System.out.println("Mapping " + X);
        UUID ret;
        while(true)
        {
            TR.BeginTX();
            if (names.containsKey(X))
                ret = names.get(X);
            else
            {
                ret = new UUID(0, idctr.read() + FIRST_VALID_STREAM_ID);
                idctr.increment();
                names.put(X, ret);
            }
            if(TR.EndTX()) break;
        }
        System.out.println("Mapped " + X + " to " + ret);
        return ret;
    }

}

