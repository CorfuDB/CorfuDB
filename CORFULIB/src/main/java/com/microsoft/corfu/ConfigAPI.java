package com.microsoft.corfu;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;

/**
 * Created by dalia on 6/4/2014.
 */
public interface ConfigAPI {

    /**
     * trim a prefix of log up to the specified position
     *
     * @param offset the position to trim to (excl)
     *
     * @throws CorfuException
     */
    public void trim(long offset) throws CorfuException;

    public void sealepoch() throws CorfuException;

    public void proposeRemoveUnit(int gind, int rind) throws CorfuException;
    public void proposeDeployUnit(int gind, int rind, String hostname, int port) throws CorfuException;
    public void killUnit(int gind, int rind) throws CorfuException;

    /**
     * recover the token server by moving it to a known lower-bound on filled position
     * should only be used by administrative utilities
     */
    public void tokenserverrecover(long lowbound) throws CorfuException;

}
