package com.microsoft.corfu;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;

/**
 * Created by dalia on 6/4/2014.
 */
public interface ConfigAPI {

    public void sealepoch() throws CorfuException;

    // public void replaceUnit(oldhost, oldport, newhost, newport) throws CorfuException
    // public void removeUnit(host, port) throws CorfuException

    public CorfuConfiguration pullConfig() throws CorfuException;

    public void proposeDeployGroup(Endpoint[] newg) throws CorfuException;
    public void proposeRemoveGroup(int groupind) throws CorfuException;
}
