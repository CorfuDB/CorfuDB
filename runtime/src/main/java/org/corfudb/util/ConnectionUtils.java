package org.corfudb.util;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.NodeRouterPool;
import org.corfudb.runtime.clients.NettyClientRouter;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Utility methods to extract ssl connection information from the client's netty channel.
 * Created by cgudisagar on 3/7/24.
 */
public class ConnectionUtils {
    @AllArgsConstructor
    @Data
    @ToString
    public static class SSLConnectionInfo {
        private boolean isChannelActive;
        private List<String> localCertificateThumbprints;
        private List<String> remoteCertificateThumbprints;

        public static SSLConnectionInfo emptyConnectionInfo() {
            return new SSLConnectionInfo(
                    false,
                    Collections.emptyList(),
                    Collections.emptyList());
        }
    }

    /**
     * Extract connection statuses using the netty channel pipeline's ssl handler
     * This method returns runtime statuses w.r.t. all the remote servers in the {@link NodeRouterPool}.
     *
     * @param corfuRuntime client runtime used to extract the connection information
     * @return A Map of server NodeLocator and its {@link SSLConnectionInfo} from the {@param corfuRuntime).
     * Note that this will be empty if there is no existing node router in runtime.
     */
    public static Map<NodeLocator, SSLConnectionInfo> extractSSLConnectionInfoMap(CorfuRuntime corfuRuntime) {
        Map<NodeLocator, SSLConnectionInfo> sslConnectionInfoMap = new HashMap<>();
        // endpoint => (channel-status, cert-thumbprints)
        corfuRuntime.getNodeRouterPool().getNodeRouters().forEach(
                (node, iClientRouter) -> sslConnectionInfoMap.put(
                        node,
                        new SSLConnectionInfo(
                                ((NettyClientRouter) iClientRouter).isChannelActive(),
                                ((NettyClientRouter) iClientRouter).getLocalCertificateThumbprints(),
                                ((NettyClientRouter) iClientRouter).getPeerCertificateThumbprints()
                        )
                )
        );
        return sslConnectionInfoMap;
    }



    /**
     * Extract connection status using the netty channel pipeline's ssl handler
     * This method returns runtime status w.r.t. only one {@link NodeLocator} of the remote server.
     *
     * @param nodeLocator node locator of the server of which the connection status is needed
     * @param corfuRuntime client runtime used to extract the connection information
     * @return {@link SSLConnectionInfo} from the {@param corfuRuntime) to the server node.
     */
    public static SSLConnectionInfo extractSSLConnectionInfo(NodeLocator nodeLocator, CorfuRuntime corfuRuntime) {
        Optional<NettyClientRouter> ncrOptional = Optional.ofNullable(
                (NettyClientRouter) corfuRuntime.getNodeRouterPool()
                        .getNodeRouters()
                        .get(nodeLocator)
        );
        // Return the value if present else return default emptyConnectionInfo
        return ncrOptional.map(
                ncr -> new SSLConnectionInfo(
                        ncr.isChannelActive(),
                        ncr.getLocalCertificateThumbprints(),
                        ncr.getPeerCertificateThumbprints()
                )
        ).orElse(SSLConnectionInfo.emptyConnectionInfo());
    }

    /**
     * Extract connection statuses of the runtime to all the server nodes in its router pool.
     *
     * @param corfuRuntime client runtime used to extract the connection information
     * @return Map containing the connection statuses of corfuRuntime to all the server nodes.
     * Note that this will be empty if there is no existing node router in runtime.
     */
    public static Map<NodeLocator, Boolean> extractIsChannelActiveMap(CorfuRuntime corfuRuntime) {
        Map<NodeLocator, Boolean> isChannelActiveMap = new HashMap<>();
        // endpoint => (channel-status, cert-thumbprint)
        corfuRuntime.getNodeRouterPool().getNodeRouters().forEach(
                (node, iClientRouter) -> isChannelActiveMap.put(
                        node,
                        ((NettyClientRouter) iClientRouter).isChannelActive()
                )
        );
        return isChannelActiveMap;
    }

    /**
     * Extract connection status of the runtime to a particular the server node in its router pool.
     *
     * @param nodeLocator  node locator of the server of which the connection status is needed
     * @param corfuRuntime client runtime used to extract the connection information
     * @return Connection status of corfuRuntime to the server node.
     */
    public static boolean extractIsChannelActive(NodeLocator nodeLocator, CorfuRuntime corfuRuntime) {
        Optional<NettyClientRouter> ncrOptional = Optional.ofNullable(
                (NettyClientRouter) corfuRuntime.getNodeRouterPool()
                        .getNodeRouters()
                        .get(nodeLocator)
        );
        // Return the value if present else return default false
        return ncrOptional.map(NettyClientRouter::isChannelActive).orElse(false);
    }

}
