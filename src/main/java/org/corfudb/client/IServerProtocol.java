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

package org.corfudb.client;

import java.util.Map;
import java.util.Map.Entry;
import java.util.HashMap;

import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.StringBuilder;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * This interface represents the minimum APIs a server protocol must implement.
 *
 * @author Michael Wei <mwei@cs.ucsd.edu>
 */

public interface IServerProtocol {

    final static Logger log = LoggerFactory.getLogger(IServerProtocol.class);
    final static Pattern r = Pattern.compile("(?<protocol>[a-z]+)\\://(?<host>(?=.{1,255}$)[0-9A-Za-z](?:(?:[0-9A-Za-z]|-){0,61}[0-9A-Za-z])?(?:\\.[0-9A-Za-z](?:(?:[0-9A-Za-z]|-){0,61}[0-9A-Za-z])?)*\\.?)\\:?(?<port>[0-9]+)?");
    /**
     * Returns the protocol string for this server protocol. For example, the protocol
     * string for the hypertext transport protocol is "http".
     */
    static String getProtocolString() {
        return "unknown";
    }

    /**
     * Returns a new instance of this protocol, given a host, port and the server string.
     *
     * @param fullstring    The full server string. This may contain special options, etc.
     *
     * @return A new instance of the protocol.
     */
    static IServerProtocol protocolFactory(Class<? extends IServerProtocol> factoryClass, String serverString, long epoch)
        throws IllegalAccessException, NoSuchMethodException, InvocationTargetException
    {
        Matcher m = getMatchesFromServerString(serverString);
        m.find();
        String host = m.group("host");
        String[] options = serverString.split(",");
        String key = null;
        HashMap<String,String> optionsmap = new HashMap<String,String>();
        for (String kvpair: options)
        {
            String[] pairsplit = kvpair.split("=");
            if (pairsplit.length > 1)
            {
                String K = pairsplit[0];
                String V = pairsplit[1];
                optionsmap.put(K, V);
            }
        }

        Integer port = null;
        try {
            port = Integer.parseInt(m.group("port"));
        }
        catch (Exception e) {}
        return (IServerProtocol) factoryClass.getMethod("protocolFactory", String.class, Integer.class, Map.class, Long.class).invoke(null, host, port, (Map<String,String>)optionsmap, epoch);
    }

    /**
     * Returns a new instance of this protocol, given a parsed host,port and optionmap.
     * Any class that implements this interface must implement and override this function.
     *
     * @param host      The hostname of the server.
     * @param port      The port number of the server.
     * @param options   A map containing the options to pass to the server.
     * @param epoch     The epoch this server must be in.
     * @return  A new instance of the protocol.
     */
    static IServerProtocol protocolFactory(String host, Integer port, Map<String,String> options, long epoch)
    {
        log.error("Apparently class does not implement protocolFactory. This is unsupported.");
        throw new UnsupportedOperationException("Must Implement static protocolFactory(host,port,options,epoch)");
    }

    /**
     * Returns a matcher containing the parsed groups from the server string.
     *
     * @param serverString  A server string to parse
     *
     * @return A matcher with the groups parsed from the regex.
     */
    static Matcher getMatchesFromServerString(String serverString)
    {
        return r.matcher(serverString);
    }

    /**
     * Returns the full server string.
     *
     * @return A server string.
     */
    default String getFullString()
    {
        StringBuilder sb = new StringBuilder();
        String protocolString = null;
        try
        {
            protocolString = (String) this.getClass().getMethod("getProtocolString").invoke(null);
        }
        catch (Exception e)
        {
            protocolString = getProtocolString();
        }
        sb.append(protocolString).append("://").append(getHost());
        if (getPort() != null) {sb.append(":").append(getPort()); }
        if (getOptions().size() > 0)
        {
            for (Entry<String,String> e : getOptions().entrySet())
            {
                sb.append(",").append(e.getKey()).append("=").append(e.getValue());
            }
        }
        return sb.toString();
    }

    /**
     * Returns the host
     *
     * @return The hostname for the server.
     */
    String getHost();

    /**
     * Returns the port
     *
     * @return The port number of the server.
     */
    Integer getPort();

    /**
     * Returns the option map
     *
     * @return The option map for the server.
     */
    Map<String,String> getOptions();

    /**
     * Returns a boolean indicating whether or not the server was reachable.
     *
     * @return True if the server was reachable, false otherwise.
     */
    boolean ping();

    /**
     * Sets the epoch of the server. Used by the configuration master to switch epochs.
     */
    void setEpoch(long epoch);
}
