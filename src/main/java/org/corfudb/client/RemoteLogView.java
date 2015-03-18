package org.corfudb.client;

import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.UUID;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.corfudb.client.configmasters.IConfigMaster;

/**
 *      This class manages and caches views of remote logs.
 */
public class RemoteLogView {

    /** This class stores data about remote logs */
    public class RemoteData {
        /** The string to the remote */
        public String remoteString;
        /** A CorfuDBView, if available */
        private CorfuDBView view;
        public RemoteData (String remoteString)
        {
            this.remoteString = remoteString;
            this.view = null;
        }
        public CorfuDBView getView()
        {
            try {
                if (view == null)
                {
                    String remoteLog = remoteString.replace("cdbcm", "http");
                    view = CorfuDBClient.retrieveView(remoteLog + "/corfu");
                    return view;
                }
            }
            catch (IOException e)
            {
                return null;
            }
            return view;
        }
    }

    public Map<UUID, RemoteData> remoteMap;
    private static final Logger log = LoggerFactory.getLogger(RemoteLogView.class);

    public RemoteLogView()
    {
        remoteMap = new ConcurrentHashMap<UUID, RemoteData>();
    }

    /**
     * Add a log to the remote view.
     *
     * @param   logID       The ID of the remote log.
     * @param   logRemote   The master string of the remote log.
     */
    public boolean addLog(UUID logID, String logRemote)
    {
        return remoteMap.putIfAbsent(logID, new RemoteData(logRemote)) == null;
    }

    /**
     * Add a log to the remote view, dynamically discovering the logID.
     *
     * @param logRemote     The master string of the remote.
     */
    public UUID addLog(String logRemote)
    {
        return addLog(logRemote, null);
    }

    /**
     * Add a log to the remote view, dynamically discovering the logID,
     * only if it is not a given UUId.
     *
     * @param logRemote     The master string of the remote.
     * @param logSelf       A UUID to ignore, null if we shouldn't care.
     */
    public UUID addLog(String logRemote, UUID logSelf)
    {
        //check if the remote is already inserted by URL.
        for (UUID id : getAllLogs())
        {
            if (remoteMap.get(id).remoteString.equals(logRemote))
            {
                return id;
            }
        }
        try {
            RemoteData rd = new RemoteData(logRemote);
            if (logSelf != null && logSelf.equals(rd.getView().getUUID()))
            {
                return logSelf;
            }
            remoteMap.putIfAbsent(rd.getView().getUUID(), new RemoteData(logRemote));
            return rd.getView().getUUID();
        }
        catch (NullPointerException ie)
        {
           log.debug("Exception dynamically discovering remote log", ie);
            return null;
        }
    }


    /**
     * Check all remote logs to see if they are still accessible.
     */
    public void checkAllLogs()
    {
        for (UUID log : getAllLogs())
        {
            checkLog(log);
        }
    }

    /**
     * Check if the log is accessible. If it is not, it is removed from the view.
     *
     * @return  True if the log was accessible, false otherwise.
     */
    public boolean checkLog(UUID logID)
    {
        try {
            CorfuDBView view = getLog(logID);
            //it only matters whether or not we can talk to the config master.
            IConfigMaster icm = (IConfigMaster)view.getConfigMasters().get(0);
            if (icm.ping())
            {
                return true;
            }
            log.info("Couldn't communicate with remote configmaster for log "+ logID + ", removing from view.");
        }
        catch (Exception ex)
        {
            log.info("Exception communicating with remote log " + logID + ", removing from view.");
        }
        remoteMap.remove(logID);
        return false;
    }

    /**
     * Get the view to the remote log.
     *
     * @param logID     The ID of the remote log.
     */
    public CorfuDBView getLog(UUID logID)
    throws RemoteException
    {
        RemoteData rd = remoteMap.get(logID);
        if (rd == null)
        {
            throw new RemoteException("Couldn't access remote log, no path to remote log.", logID);
        }
        else
        {
            return rd.getView();
        }
    }

    /**
     * Get the configuration string of the remote log.
     *
     * @param logID     The ID of the remote log.
     */
    public String getLogString(UUID logID)
    throws RemoteException
    {
        RemoteData rd = remoteMap.get(logID);
        if (rd == null)
        {
            throw new RemoteException("Couldn't access remote log, no path to remote log.", logID);
        }
        else
        {
            return rd.remoteString;
        }
    }


    /**
     * Get a list of all remote logs in the view and their mappings
     */
    public Map<UUID, String> getAllLogsMappings()
    {
        Map<UUID, String> output = new HashMap<UUID, String>();

        for (UUID logid : getAllLogs())
        {
            output.put(logid, remoteMap.get(logid).remoteString);
        }

        return output;
    }

    /**
     * Get a list of all remote logs in the view.
     *
     * @return      A set containing all the remote logs in the view.
     */
    public Set<UUID> getAllLogs()
    {
        return remoteMap.keySet();
    }

}
