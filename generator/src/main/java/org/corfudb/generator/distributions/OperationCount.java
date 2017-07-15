package org.corfudb.generator.distributions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * Created by maithem on 7/14/17.
 */
public class OperationCount extends DataSet {
    private Random rand = new Random();

    public void populate() {
        //no-op
    }

    public List<Integer> sample(int num) {
        List<Integer> ints = new ArrayList<>();
        for (int x = 0; x < num; x++) {
            ints.add(rand.nextInt(50) + 1);
        }
        return ints;
    }

    public List getDataSet() {
        return Arrays.asList();
    }
}
