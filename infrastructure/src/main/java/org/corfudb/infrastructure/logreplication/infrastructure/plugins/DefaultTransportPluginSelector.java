package org.corfudb.infrastructure.logreplication.infrastructure.plugins;

import org.apache.commons.lang3.tuple.Pair;

public class DefaultTransportPluginSelector implements LogReplicationTransportSelectorPlugin{

    // TODO V2: when netty is bought back, use system.property to indicate the sample transport plugin to be used
    public Pair<String, String> getTransportPropertyToReadFromConfig() {
        return Pair.of("GRPC_transport_adapter_client_class_name", "GRPC_transport_adapter_server_class_name");
    }
}
