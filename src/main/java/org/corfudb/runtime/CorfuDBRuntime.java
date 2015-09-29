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

import org.corfudb.runtime.protocols.configmasters.MemoryConfigMasterProtocol;
import org.corfudb.runtime.stream.IStream;
import org.corfudb.runtime.view.*;
import org.corfudb.util.GitRepositoryState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import javax.json.JsonReader;
import javax.json.Json;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

import java.lang.reflect.InvocationTargetException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;

import org.corfudb.runtime.protocols.configmasters.IConfigMaster;

/**
 * Note, the following imports require Java 8
 */
import java.util.concurrent.locks.StampedLock;

/**
 * This class is used by clients to access the CorfuDB infrastructure.
 * It is responsible for constructing client views, and returning
 * interfaces which clients use to access the CorfuDB infrastructure.
 *
 * @author Michael Wei <mwei@cs.ucsd.edu>
 */
public class CorfuDBRuntime implements AutoCloseable {

    private String configurationString;
    private StampedLock viewLock;
    private Thread viewManagerThread;
    private org.corfudb.runtime.view.CorfuDBView currentView;
    private BooleanLock viewUpdatePending;
    private Boolean closed = false;
    private UUID localID = null;
    private org.corfudb.runtime.view.RemoteLogView remoteView;
    private LocalDateTime lastInvalidation;
    private LocalDateTime backOffTime;
    private long backOff = 0;
    private static final HashMap<String, CorfuDBRuntime> s_rts = new HashMap();

    private ICorfuDBInstance localInstance;

    private static final Logger log = LoggerFactory.getLogger(CorfuDBRuntime.class);

    private class BooleanLock
    {
        public boolean lock;
        public BooleanLock() {
            lock = false;
        }
    }

    /**
     * Suppressed default constructor.
     */
    private CorfuDBRuntime() {}

    /**
     * Constructor. Generates an instance of a CorfuDB client, which
     * manages views of the CorfuDB infrastructure and provides interfaces
     * for clients to access.
     *
     * @param configurationString   A configuration string which describes how to reach the
     *                              CorfuDB instance. This is usually a http address for a
     *                              configuration master.
     *                              The string "memory" is also acceptable, it will generate
     *                              a local in memory instance of CorfuDB with a single
     *                              stream unit and sequencer.
     */
    public CorfuDBRuntime(String configurationString) {
        this.configurationString = configurationString;

        viewLock = new StampedLock();
        remoteView = new org.corfudb.runtime.view.RemoteLogView();
        viewUpdatePending = new BooleanLock();
        viewUpdatePending.lock = true;
        viewManagerThread = getViewManagerThread();
        try {
            localInstance = new LocalCorfuDBInstance(this);
        } catch (Exception e)
        {
            log.error("Exception creating local instance", e);
        }
        startViewManager();
    }

    /**
     * Get the version of this client binding.
     * @return  A string describing the version of this client binding.
     */
    public String getVersion() {
        return GitRepositoryState.getRepositoryState().describe;
    }

    /**
     * interface for acquiring singleton runtime.
     * @param configurationString
     * @return
     */
    public static CorfuDBRuntime getRuntime(String configurationString) {
        synchronized (s_rts) {
            CorfuDBRuntime rt = s_rts.getOrDefault(configurationString, null);
            if(rt == null) {
                rt = new CorfuDBRuntime(configurationString);
                s_rts.put(configurationString, rt);
            }
            return rt;
        }
    }

    /**
     * API for acquiring a runtime object. Generally, CorfuDBRuntime should
     * be a singleton, and getRuntime is the way to get a reference to said
     * singleton. However, there are scenarios (e.g. tests) where forcing the
     * creation of a fresh runtime object is actually the right thing to do.
     * TODO: refactor to discourage use of this API by user code.
     * @param configurationString
     * @return
     */
    public static CorfuDBRuntime createRuntime(String configurationString) {
        synchronized (s_rts) {
            CorfuDBRuntime rt = new CorfuDBRuntime(configurationString);
            s_rts.put(configurationString, rt);
            return rt;
        }
    }


    /**
     * Starts the view manager thread. The view manager retrieves the view
     * and manages view changes. This thread will be automatically started
     * when any requests are made, but this method allows the view manager
     * to load the initial view during application load.
     */
    public void startViewManager() {
        if (!viewManagerThread.isAlive())
        {
            log.debug("Starting view manager thread.");
            viewManagerThread.start();
        }
    }


    public ICorfuDBInstance getLocalInstance()
    {
        return localInstance;
    }

    /**
     * Retrieves the CorfuDBView from a configuration string. The view manager
     * uses this method to fetch the most recent view.
     */
    public static org.corfudb.runtime.view.CorfuDBView retrieveView(String configString)
        throws IOException {
        if (configString.equals("custom"))
        {
            if (MemoryConfigMasterProtocol.memoryConfigMasters.get(0) != null)
            {
                return MemoryConfigMasterProtocol.memoryConfigMasters.get(0).getView();
            }
        }
        else if (configString.equals("memory"))
        {
            if (MemoryConfigMasterProtocol.memoryConfigMasters.get(0) != null)
            {
                return MemoryConfigMasterProtocol.memoryConfigMasters.get(0).getView();
            }
            //this is an in-memory request.
            HashMap<String, Object> MemoryView = new HashMap<String, Object>();
            MemoryView.put("epoch", 0L);

            MemoryView.put("logid", UUID.randomUUID().toString());
            MemoryView.put("pagesize", 4096);

            LinkedList<String> configMasters = new LinkedList<String>();
            configMasters.push("mcm://localhost:0");
            MemoryView.put("configmasters", configMasters);

            LinkedList<String> sequencers = new LinkedList<String>();
            sequencers.push("ms://localhost:0");
            MemoryView.put("sequencers", sequencers);

            HashMap<String,Object> layout = new HashMap<String,Object>();
            LinkedList<HashMap<String,Object>> segments = new LinkedList<HashMap<String,Object>>();
            HashMap<String,Object> segment = new HashMap<String,Object>();
            segment.put("replication", "cdbcr");
            segment.put("start", 0L);
            segment.put("sealed", 0L);
            LinkedList<HashMap<String,Object>> groups = new LinkedList<HashMap<String,Object>>();
            HashMap<String,Object> group0 = new HashMap<String,Object>();
            LinkedList<String> group0nodes = new LinkedList<String>();
            group0nodes.add("mlu://localhost:0");
            group0.put("nodes", group0nodes);
            groups.add(group0);
            segment.put("groups", groups);
            segments.add(segment);
            layout.put("segments", segments);
            MemoryView.put("layout", layout);
            CorfuDBView newView = new CorfuDBView(MemoryView);
            MemoryConfigMasterProtocol.memoryConfigMasters.get(0).setInitialView(newView);
            return MemoryConfigMasterProtocol.memoryConfigMasters.get(0).getView();
        }

        URL url = new URL(configString);
        HttpURLConnection huc = (HttpURLConnection) url.openConnection();
        huc.setRequestProperty("Content-Type", "application/json");
        huc.connect();
        try (InputStream is = (InputStream) huc.getContent()) {
            try (JsonReader jr = Json.createReader(new BufferedReader(new InputStreamReader(is)))) {
                return new org.corfudb.runtime.view.CorfuDBView(jr.readObject());
            }
        }
    }

    /**
     * Invalidate the current view and wait for a new view.
     */
    public void invalidateViewAndWait(NetworkException e)
    {
        log.warn("Client requested invalidation of current view, backoff level=" + backOff);
        lastInvalidation = LocalDateTime.now();

        if (backOffTime != null)
        {
            if (backOffTime.compareTo(lastInvalidation) > 0)
            {
                //backoff!
                try {
                    Thread.sleep((long) Math.pow(2, backOff) * 1000);
                } catch (Exception ex) {}
                //increment backoff
                backOff++;
                backOffTime = LocalDateTime.now().plus((long)Math.pow(2, backOff), ChronoUnit.SECONDS);
            }
            else
            {
                //no need to backoff
                backOff = 0;
            }
        }
        else
        {
            backOff++;
            backOffTime = LocalDateTime.now().plus((long)Math.pow(2, backOff), ChronoUnit.SECONDS);
        }

        if (currentView != null)
        {
            currentView.invalidate();
            IConfigurationMaster cm = new ConfigurationMaster(this);
            cm.requestReconfiguration(e);
        }
            synchronized(viewUpdatePending)
            {
                viewUpdatePending.lock = true;
                viewUpdatePending.notify();
                while (viewUpdatePending.lock)
                {
                    try {
                    viewUpdatePending.wait();
                    } catch (InterruptedException ie)
                    {}
                }
            }
    }

    /**
     * Synchronously block until a valid view is installed.
     */
    public void waitForViewReady()
    {
        synchronized(viewUpdatePending)
        {
            while (viewUpdatePending.lock)
            {
                try {
                viewUpdatePending.wait();
                } catch (InterruptedException ie)
                {}
            }
        }
    }

    /**
     * Get the view for a remote stream.
     *
     * @param logID     The UUID of the remote stream to retrieve the view for.
     *
     * @return          A CorfuDBView, if the remote stream can be retrieved, or
     *                  null, if the UUID cannot be resolved.
     */
    public org.corfudb.runtime.view.CorfuDBView getView(UUID logID)
    throws RemoteException
    {
        if (logID == null) {return getView();}
        if (logID.equals(localID)) { return getView(); }
        /** Go to the current view, and communicate with the local configuration
         *  master to resolve the stream.
         */
        try{
            return remoteView.getLog(logID);
        }
        catch (RemoteException re)
        {
            IConfigMaster cm = (IConfigMaster) getView().getConfigMasters().get(0);
            String remoteLog = cm.getLog(logID);
            /** Go to the remote stream, communicate with the remote configuration master
             * and resolve the remote configuration.
             */
            remoteLog = remoteLog.replace("cdbcm", "http");
            remoteView.addLog(logID, remoteLog);
            return remoteView.getLog(logID);
        }
    }

    /**
     * Get the current view. This method optimisically acquires the
     * current view.
     */
    public org.corfudb.runtime.view.CorfuDBView getView()
    {
        if (viewManagerThread == null || currentView == null || !currentView.isValid())
        {
            if (viewManagerThread == null)
            {
                startViewManager();
            }
            synchronized(viewUpdatePending)
            {
                while (viewUpdatePending.lock)
                {
                    try {
                    viewUpdatePending.wait();
                    } catch (InterruptedException ie)
                    {}
                }
            }
        }
        long stamp = viewLock.tryOptimisticRead();
        org.corfudb.runtime.view.CorfuDBView view = currentView;
        if (!viewLock.validate(stamp))
        {
            //We should only get here if the view is being updated.
            stamp = viewLock.readLock();
            view = currentView;
            viewLock.unlock(stamp);
        }
        return currentView;
    }

    public void close()
    {
        closed = true;
        viewManagerThread.interrupt();
    }
    /**
     * Retrieves a runnable that provides a view manager thread. The view
     * manager retrieves the view and manages view changes.
     */
    private Thread getViewManagerThread() {
        return new Thread(new Runnable() {
            @Override
            public void run() {
                log.debug("View manager thread started.");
                while (!closed)
                {
                    synchronized(viewUpdatePending)
                    {
                        if (viewUpdatePending.lock)
                        {
                            log.debug("View manager retrieving view...");
                            //lock, preventing old view from being read.
                            long stamp = viewLock.writeLock();
                            try {
                                org.corfudb.runtime.view.CorfuDBView newView = retrieveView(configurationString);
                                if (currentView == null || newView.getEpoch() > currentView.getEpoch())
                                {
                                    String oldEpoch = (currentView == null) ? "null" : Long.toString(currentView.getEpoch());
                                    log.info("New view epoch " + newView.getEpoch() + " greater than old view epoch " + oldEpoch + ", changing views");
                                    currentView = newView;
                                    localID = currentView.getUUID();
                                    viewUpdatePending.lock = false;
                                    viewUpdatePending.notifyAll();
                                }
                                else
                                {
                                    log.info("New view epoch " + newView.getEpoch() + " is the same as previous...");
                                    currentView.revalidate();
                                    viewUpdatePending.lock = false;
                                    viewUpdatePending.notifyAll();
                                }
                            }
                            catch (IOException ie)
                            {
                                log.warn("Error retrieving view at " + configurationString + ": " + ie.getMessage());
                                if (currentView != null) {currentView.invalidate();}
                            }
                            finally {
                                viewLock.unlock(stamp);
                            }
                        }
                        else
                        {
                            while (!viewUpdatePending.lock)
                            {
                                try {
                                    viewUpdatePending.wait();
                                }
                                catch (InterruptedException ie){
                                    if (closed) { return; }
                                }
                            }
                        }
                    }
                }
            }
        });
    }
}
