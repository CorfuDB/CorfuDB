package org.corfudb.generator.distributions;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * This class implements the distribution of keys that can be inserted
 * into a map.
 * <p>
 * Created by maithem on 7/14/17.
 */
public class Keys implements DataSet<String> {
    private final Set<String> mapKeys;
    private final int numKeys;

    public Keys(int num) {
        mapKeys = new HashSet<>();
        numKeys = num;
    }

    @Override
    public void populate() {
        for (int x = 0; x < numKeys; x++) {
            mapKeys.add("key_" + x);
        }
    }

    @Override
    public List<String> getDataSet() {
        return new ArrayList<>(mapKeys);
    }
}
