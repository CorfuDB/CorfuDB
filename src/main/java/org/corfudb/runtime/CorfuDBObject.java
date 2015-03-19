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

import edu.umd.cs.findbugs.annotations.NoWarning;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * This class must be sub-classed by any new CorfuDB object class. It provides
 * helper code for locking and timestamp bookkeeping. It also defines the abstract
 * apply upcall that every CorfuDB object must implement.
 */
public abstract class CorfuDBObject implements Comparable<CorfuDBObject>
{
    public static final long oidnull = -1;

    //object ID -- corresponds to stream ID used underneath
    public long oid;
    public AbstractRuntime TR;

    ITimestamp timestamp = TimestampConstants.singleton().getMinTimestamp();

    public ITimestamp getTimestamp()
    {
        return getTimestamp(null);
    }

    public ITimestamp getTimestamp(Serializable key)
    {
        return timestamp;
    }

    public void setTimestamp(ITimestamp newts)
    {
        setTimestamp(newts, null);
    }

    public void setTimestamp(ITimestamp newts, Serializable key)
    {
        if(timestamp.equals(TimestampConstants.singleton().getInvalidTimestamp()))
            throw new RuntimeException("cannot set to invalid timestamp!");
        timestamp = newts;
    }

    abstract public void applyToObject(Object update, ITimestamp timestamp) throws Exception;

    //override this in subclass to perform custom conflict detection
    public boolean isStillValid(Serializable readsummary)
    {
        throw new RuntimeException("unimplemented! override this in subclass...");
    }

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
        System.out.println("Creating CorfuDBObject " + tobjectid);
        TR = tTR;
        oid = tobjectid;
        TR.registerObject(this, remote);
        statelock = new ReentrantReadWriteLock();
    }

    public int compareTo(CorfuDBObject compareCob) {
        // using identity hash code is robust, but is more mechanism
        // than is really needed here. OIDs have to be unique for
        // basic services to work, so it suffices to use the oid instead:
        // int thisHashCode = System.identityHashCode(this);
        // int thatHashCode = System.identityHashCode(compareCob);
        // return thisHashCode - thatHashCode;
        long result = (this.oid - compareCob.oid);
        return result == 0 ? 0 : result > 0 ? 1 : -1;
    }

    /*
     * synchronization support.
     * cjr: 3/3/2015:
     * Refactored slightly to simplify the debug infrastructure added below.
     * The refactoring preserves the original semantics by default, which were,
     * frankly, a little weird to me, since un-parameterized calls to lock/unlock
     * default to requesting a read-lock. The safe path seems to me to have
     * simple lock/unlock to be conservative and enforce full mutual exclusion.
     * A flag below toggles that behavior, but for now, we still default to
     * read locks unless write lock is explicitly requested.
     */

    ReadWriteLock statelock;
    public static final boolean REQUEST_READ_LOCK = false;
    public static final boolean REQUEST_WRITE_LOCK = true;
    public static final boolean DEFAULT_LOCK_TYPE = REQUEST_READ_LOCK;

    public void lock() { lock(DEFAULT_LOCK_TYPE); }
    public void unlock() { unlock(DEFAULT_LOCK_TYPE); }
    public void lock(boolean write) { if(write) wlock(); else rlock(); }
    public void unlock(boolean write) { if(write) wunlock(); else runlock(); }

    /*
     * code below is essentially debug infrastructure: these tools make it
     * easier to debug concurrency problems by making it easier to figure out
     * which threads think they have which locks, tracking depth locks are re-entrant,
     * which thread holds the lock etc. Depth counters are tracked in thread local
     * storage, which is arguably unnecessary, but in my experience the extra complexity
     * is helpful, since they require no synchronization, and are therefore immune to
     * the concurrency bugs they are supposed to help ferret out.
     * In C/C++/C#, this stuff is super-easy to compile out for performance evaluation.
     * In Java, there is no conditional compilation support (sigh), so there is some
     * TBD work to determine how to get this stuff out of a release build.
     */

    public static final long NOOWNINGTHREAD = -1;
    protected TLSCounter m_wlockdepth = new TLSCounter();
    protected TLSCounter m_rlockdepth = new TLSCounter();
    protected long m_ownerthread = NOOWNINGTHREAD;

    public boolean rlockheld() { return m_rlockdepth.getval() > 0; }
    public boolean wlockheld() { return m_wlockdepth.getval() > 0; }
    public boolean lockheld() { return rlockheld() || wlockheld(); }

    public void rlock() {
        statelock.readLock().lock();
        Integer readers = m_rlockdepth.getval();
        long tid = Thread.currentThread().getId();
        assert(readers >= 0);
        assert((readers == 0 && m_ownerthread == NOOWNINGTHREAD) || m_ownerthread == tid);
        assert(m_wlockdepth.getval() == 0);
        readers++;
        m_rlockdepth.set(readers);
        m_ownerthread = tid; // shoudl be idempotent.
    }

    public void runlock() {
        long tid = Thread.currentThread().getId();
        Integer readers = m_rlockdepth.getval();
        assert(readers > 0);
        assert(tid == m_ownerthread);
        assert(m_wlockdepth.getval() == 0);
        if(--readers == 0) m_ownerthread = NOOWNINGTHREAD;
        m_rlockdepth.set(readers);
        statelock.readLock().unlock();
    }

    public void wlock() {
        statelock.writeLock().lock();
        long tid = Thread.currentThread().getId();
        int writers = m_wlockdepth.getval();
        assert(m_rlockdepth.getval() == 0);
        assert(writers >= 0);
        assert(writers == 0 && m_ownerthread == NOOWNINGTHREAD || tid == m_ownerthread);
        m_wlockdepth.set(++writers);
        m_ownerthread = tid;
    }

    public void wunlock() {
        long tid = Thread.currentThread().getId();
        int writers = m_wlockdepth.getval();
        assert(writers > 0);
        assert(m_rlockdepth.getval() == 0);
        assert(tid == m_ownerthread);
        if(--writers == 0) m_ownerthread = NOOWNINGTHREAD;
        m_wlockdepth.set(writers);
        statelock.writeLock().unlock();
    }

}

