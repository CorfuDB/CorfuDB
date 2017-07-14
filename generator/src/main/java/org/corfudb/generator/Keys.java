package org.corfudb.generator;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Created by maithem on 7/14/17.
 */
public class Keys extends DataSet {
    final private Set<UUID> keys;
    final private int numKeys;

    public Keys(int num) {
        keys = new HashSet<>();
        numKeys = num;
    }

    @Override
    public void populate() {
        for (int x = 0; x < numKeys; x++) {
            keys.add(UUID.randomUUID());
        }
    }

    @Override
    public List<UUID> getDataSet() {
        return new ArrayList<>(keys);
    }
}
