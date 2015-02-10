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
    /**
     * This function is to be called within any accessor method in the CorfuDB object.
     *
     * Outside a transactional context it returns after syncing until the current tail of
     * the underlying SMR stream bundle (in the process, it calls the CorfuDB object's apply
     * upcall.
     *
     * Within a transactional context it returns immediately after modifying the
     * read-set of the transaction.
     *
     * @param cob the calling CorfuDB object
     */
    void query_helper(CorfuDBObject cob);
    void query_helper(CorfuDBObject cob, Serializable key);
    void query_helper(CorfuDBObject cob, Serializable key, CorfuDBObjectCommand command);

    /**
     * This function is to be called within any mutator method in the CorfuDB object.
     *
     * Outside a transactional context it returns after appending the update to the underlying
     * SMR stream bundle.
     *
     * Within a transactional context it returns immediately after buffering the update.
     *  @param cob the calling CorfuDB object
     * @param update a serializable description of the update
     */
    void update_helper(CorfuDBObject cob, CorfuDBObjectCommand update);


    void update_helper(CorfuDBObject cob, CorfuDBObjectCommand update, Serializable key);


    /**
     * This function is to be called within any method that first accesses, then
     * mutates the object state. Accordingly it takes a query command and a
     * a serializable update parameter.
     *
     * Outside a transactional context it returns after appending the update to the stream bundle;
     * playing the stream bundle until the append point (and calling apply on encountered updates);
     * applying the query to the object; and then applying the appended update to the object. This has
     * the effect of executing both the query and the update at the same point in the total order,
     * ensuring that they seem to occur atomically.
     *
     * Within a transactional context it buffers the update, uses the query to mark the read set, applies
     * the query, and returns without issuing any I/O to the underlying stream bundle.
     *  @param cob the calling CorfuDB object
     * @param query a description of the query
     * @param update a serializable description of the update
     */
    void query_then_update_helper(CorfuDBObject cob, CorfuDBObjectCommand query, CorfuDBObjectCommand update);

    void query_then_update_helper(CorfuDBObject cob, CorfuDBObjectCommand query, CorfuDBObjectCommand update, Serializable key);

    /**
     * This function is used to register a CorfuDB object with the runtime. Future updates that
     * belong to a particular stream ID are directed to the object with the same object ID.
     *
     * @param obj
     */
    void registerObject(CorfuDBObject obj);

    void registerObject(CorfuDBObject obj, boolean remote);

    /**
     * Starts a new transaction tied to the executing thread. Transactions cannot span threads.
     */
    void BeginTX();

    /**
     * Attempts to commit a transaction.
     *
     * @return true if the transaction commits, false otherwise.
     */
    boolean EndTX();


}
