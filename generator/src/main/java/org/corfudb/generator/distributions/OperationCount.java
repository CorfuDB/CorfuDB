package org.corfudb.generator.distributions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * This class implements the distribution of a random integer generator.
 *
 * Created by maithem on 7/14/17.
 */
public class OperationCount implements DataSet {
    private Random rand = new Random();

    public void populate() {
        //no-op
    }

    @Override
    public List<Integer> sample(int num) {
        List<Integer> ints = new ArrayList<>();
        for (int x = 0; x < num; x++) {
            ints.add(rand.nextInt(100) + 1);
        }
        return ints;
    }

    public List getDataSet() {
        return Arrays.asList();
    }
}
