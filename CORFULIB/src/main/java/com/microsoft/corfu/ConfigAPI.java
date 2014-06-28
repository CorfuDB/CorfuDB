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

    public void proposeRemoveUnit(int gind, int rind) throws CorfuException;
    public void proposeDeployUnit(int gind, int rind, String hostname, int port) throws CorfuException;
}
