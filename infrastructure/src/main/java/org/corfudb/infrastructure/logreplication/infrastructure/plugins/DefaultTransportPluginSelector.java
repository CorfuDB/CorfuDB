package org.corfudb.infrastructure.logreplication.infrastructure.plugins;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;

@Slf4j
public class DefaultTransportPluginSelector implements LogReplicationTransportSelectorPlugin{

    public static final String TRANSPORT_TYPE_ENV_VARIABLE = "TransportType";

    public Pair<String, String> getTransportPropertyToReadFromConfig() {
        if (System.getenv(TRANSPORT_TYPE_ENV_VARIABLE) != null && System.getenv(TRANSPORT_TYPE_ENV_VARIABLE).equals("NETTY")) {
            return Pair.of("NETTY_transport_adapter_client_class_name", "NETTY_transport_adapter_server_class_name");
        }
        return Pair.of("GRPC_transport_adapter_client_class_name", "GRPC_transport_adapter_server_class_name");
    }
}
