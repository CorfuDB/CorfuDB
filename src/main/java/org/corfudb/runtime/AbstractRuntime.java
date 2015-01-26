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

import java.io.Serializable;
import java.util.Set;

/**
 * This is the interface exposed by CorfuDB to developers of new objects.
 *
 */
public interface AbstractRuntime
{
    void query_helper(CorfuDBObject cob);
    void update_helper(CorfuDBObject cob, Serializable update, Set<Long> streams);
    void query_then_update_helper(CorfuDBObject cob, Object query, Serializable update, Set<Long> streams);
    void registerObject(CorfuDBObject obj);
}
