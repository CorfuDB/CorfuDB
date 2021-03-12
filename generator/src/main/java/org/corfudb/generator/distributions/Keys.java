package org.corfudb.generator.distributions;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;

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
public class Keys implements DataSet<Keys.KeyId> {
    private final Set<KeyId> mapKeys;
    private final int numKeys;

    public Keys(int num) {
        mapKeys = new HashSet<>();
        numKeys = num;
    }

    @Override
    public void populate() {
        for (int key = 0; key < numKeys; key++) {
            mapKeys.add(new KeyId(key));
        }
    }

    @Override
    public List<KeyId> getDataSet() {
        return new ArrayList<>(mapKeys);
    }

    @EqualsAndHashCode
    @AllArgsConstructor
    public static class KeyId {
        private final int key;

        public String getKey(){
            return "key_" + key;
        }
    }
}
