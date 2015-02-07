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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * This class must be sub-classed by any new CorfuDB object class. It provides
 * helper code for locking and timestamp bookkeeping. It also defines the abstract
 * apply upcall that every CorfuDB object must implement.
 */
public abstract class CorfuDBObject
{
    //object ID -- corresponds to stream ID used underneath
    public long oid;
    ReadWriteLock statelock;
    public AbstractRuntime TR;

    AtomicLong timestamp;

    public long getTimestamp()
    {
        return getTimestamp(null);
    }

    public long getTimestamp(Serializable key)
    {
        return timestamp.get();
    }

    public void setTimestamp(long newts)
    {
        setTimestamp(newts, null);
    }

    public void setTimestamp(long newts, Serializable key)
    {
        timestamp.set(newts);
    }

    public void lock()
    {
        lock(false);
    }

    public void unlock()
    {
        unlock(false);
    }

    public void lock(boolean write)
    {
        if(write) statelock.writeLock().lock();
        else statelock.readLock().lock();
    }

    public void unlock(boolean write)
    {
        if(write) statelock.writeLock().unlock();
        else statelock.readLock().unlock();
    }

    abstract public void applyToObject(Object update);

    public long getID()
    {
        return oid;
    }

    public CorfuDBObject(AbstractRuntime tTR, long tobjectid)
    {
        this(tTR, tobjectid, false);
    }

    public CorfuDBObject(AbstractRuntime tTR, long tobjectid, boolean remote)
    {
        TR = tTR;
        oid = tobjectid;
        System.out.println("registering... " + remote);
        TR.registerObject(this, remote);
        timestamp = new AtomicLong();
        statelock = new ReentrantReadWriteLock();
    }

}

