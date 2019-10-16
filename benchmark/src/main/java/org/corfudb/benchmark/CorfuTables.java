package org.corfudb.benchmark;

import lombok.Getter;
import org.corfudb.runtime.collections.CorfuTable;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class CorfuTables {
    final int numTables;
    @Getter
    final Map<UUID, CorfuTable> maps;

    CorfuTables(int numTables, HashMap<UUID, CorfuTable> maps) {
        this.numTables = numTables;
        this.maps = maps;
    }

    protected CorfuTable getTable(UUID uuid) {
        return maps.get(uuid);
    }
}