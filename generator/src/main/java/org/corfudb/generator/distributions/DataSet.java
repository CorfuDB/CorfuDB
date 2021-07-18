package org.corfudb.generator.distributions;

import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * This class defines a generic data distribution. A DataSet is used by operations
 * that require data to execute (i.e. writing).
 * <p>
 * Created by maithem on 7/14/17.
 */
public interface DataSet<T> {
    Random RANDOM = new Random();

    /**
     * Populate the data set.
     */
    void populate();

    /**
     * Returns a random subset of the data. The default sampling method
     * uses a uniform distribution, but other distributions can be used
     * by the concrete classes.
     *
     * @param num number of data points
     * @return random data points
     */
    default List<T> sample(int num) {
        final List<T> dataSet = getDataSet();
        final int bound = dataSet.size();

        return IntStream.range(0, num)
                .mapToObj(index -> dataSet.get(RANDOM.nextInt(bound)))
                .collect(Collectors.toList());
    }

    default T sample() {
        return sample(1).get(0);
    }

    /**
     * Return the whole data set.
     *
     * @return Return a list of all the data points
     */
    List<T> getDataSet();
}
