package org.corfudb.runtime.smr.legacy;

import org.corfudb.runtime.HoleEncounteredException;
import org.corfudb.runtime.smr.Pair;
import org.corfudb.runtime.smr.legacy.SMRLearner;
import org.corfudb.runtime.smr.Triple;
import org.corfudb.runtime.stream.IStream;
import org.corfudb.runtime.stream.ITimestamp;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by crossbach on 5/22/15.
 */
public class RemoteReadMap implements IRemoteReadMap, SMRLearner
{
    //this can't be implemented over CorfuDBObjects to avoid
    //circularity issues.

    //for now, we maintain just one node per object
    Map<UUID, Pair<String, Integer>> objecttoruntimemap;
    Lock biglock;

    SMREngine smre;

    public RemoteReadMap(IStream s, UUID uniquenodeid)
    {
        objecttoruntimemap = new HashMap<UUID, Pair<String, Integer>>();
        smre = new SMREngine(s, uniquenodeid);
        smre.registerLearner(this);
        biglock = new ReentrantLock();
    }

    @Override
    public Pair<String, Integer> getRemoteRuntime(UUID objectid)
    {
        try {
            smre.sync();
        } catch(HoleEncounteredException he) {
            throw new RuntimeException(he);
        } catch(IOException ioe) {
            throw new RuntimeException(ioe);
        }

        biglock.lock();
        Pair<String, Integer> P = objecttoruntimemap.get(objectid);
        if(P==null)
        {
            System.out.println("object " + objectid + " not found");
            System.out.println(objecttoruntimemap);
        }
        biglock.unlock();
        return P;
    }

    @Override
    public void putMyRuntime(UUID objectid, String hostname, int port)
    {
        Triple T = new Triple(objectid, hostname, port);
        try {
            smre.propose(T);
        } catch(HoleEncounteredException he) {
            throw new RuntimeException(he);
        } catch(IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    @Override
    public void deliver(Object command, UUID curstream, ITimestamp timestamp)
    {
        Triple<UUID, String, Integer> T = (Triple<UUID, String, Integer>)command;
        biglock.lock();
        objecttoruntimemap.put(T.first, new Pair(T.second, T.third));
        biglock.unlock();
    }
}