package org.corfudb.runtime.stream;

import org.junit.Test;
import static org.assertj.core.api.Assertions.*;
import static com.github.marschall.junitlambda.LambdaAssert.assertRaises;

/**
 * Created by mwei on 4/30/15.
 */
public class SimpleTimestampTest {
    @Test
    public void TestComparisons() {
        assertThat((ITimestamp) new SimpleTimestamp(0))
                .isLessThanOrEqualTo(new SimpleTimestamp(0))
                .isLessThan(new SimpleTimestamp(1))
                .isLessThan(new SimpleTimestamp(Long.MAX_VALUE))
                .isGreaterThan(ITimestamp.getMinTimestamp())
                .isLessThan(ITimestamp.getMaxTimestamp());

        assertThat((ITimestamp)new SimpleTimestamp(600))
                .isEqualTo(new SimpleTimestamp(600))
                .isLessThan(new SimpleTimestamp(1000))
                .isGreaterThan(new SimpleTimestamp(Long.MIN_VALUE))
                .isGreaterThan(new SimpleTimestamp(0))
                .isGreaterThan(ITimestamp.getMinTimestamp());

        assertThat((ITimestamp) ITimestamp.getMinTimestamp())
                .isLessThan(new SimpleTimestamp(0))
                .isLessThan(new SimpleTimestamp(Long.MIN_VALUE));

        assertThat((ITimestamp) ITimestamp.getMaxTimestamp())
                .isGreaterThan(new SimpleTimestamp(0))
                .isGreaterThan(new SimpleTimestamp(Long.MAX_VALUE));

        assertRaises(() -> new SimpleTimestamp(10).compareTo(ITimestamp.getInvalidTimestamp()),
                ClassCastException.class);

        assertRaises(() -> new SimpleTimestamp(10).compareTo(null),
                ClassCastException.class);

        assertRaises(() -> ITimestamp.getInvalidTimestamp().compareTo(new SimpleTimestamp(93)),
                ClassCastException.class);
    }
}
