package org.corfudb.generator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * Created by maithem on 7/14/17.
 */
public abstract class DataSet {

    public abstract void populate();

    public List sample(int num) {
        getDataSet().size();

        List<UUID> list = new ArrayList<>();

        if (num > getDataSet().size()) {
            for (int x = 0; x < (num / getDataSet().size()) + 1; x++) {
                list.addAll(getDataSet());
            }
        } else {
            list.addAll(getDataSet());
        }

        Collections.shuffle(list);
        return list.subList(0, num);
    }

    public abstract List getDataSet();
}
