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

/**
 * This interface represents the minimum APIs a server protocol must implement.
 *
 * @author Michael Wei <mwei@cs.ucsd.edu>
 */

public interface IServerProtocol {

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
     * @param host  The address of the host.
     * @param port  The port number the protocol is being serviced on, if applicable.
     * @param fullstring    The full server string. This may contain special options, etc.
     */
    static IServerProtocol protocolFactory(String host, String port, String fullString)
    {
        throw new UnsupportedOperationException("The protocol must implement protocolFactory");
    }

    /**
     * Returns the full server string.
     */
    String getFullString();

    /**
     * Returns a boolean indicating whether or not the server was reachable.
     */
    boolean ping();

}
