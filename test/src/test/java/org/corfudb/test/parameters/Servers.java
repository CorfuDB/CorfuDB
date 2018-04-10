package org.corfudb.test.parameters;

import lombok.Getter;
import org.corfudb.util.NodeLocator;
import org.corfudb.util.NodeLocator.Protocol;

// Magic number check disabled to make this constants more readable.
@SuppressWarnings("checkstyle:magicnumber")
public enum Servers {
    SERVER_0(0),
    SERVER_1(1),
    SERVER_2(2),
    SERVER_3(3),
    SERVER_4(4),
    SERVER_5(5),
    SERVER_6(6),
    SERVER_7(7),
    SERVER_8(8),
    SERVER_9(9)
    ;

    static final String SERVER_HOST = "server";

    @Getter
    final NodeLocator locator;

    Servers(int port) {
        locator = NodeLocator.builder()
            .protocol(Protocol.LOCAL)
            .host(SERVER_HOST)
            .port(port)
            .build();
    }
}