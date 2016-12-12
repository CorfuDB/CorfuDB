package org.corfudb.runtime.collections;

import com.google.common.reflect.TypeToken;
import groovy.lang.DelegatesTo;
import org.corfudb.AbstractCorfuTest;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.view.Layout;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by mwei on 1/8/16.
 */
public class ReplexSMRMapTest extends AbstractCorfuTest {

    /** Replex tests are disabled until unit tests stabilize
     * TODO: Restore & fix up test as of commit 3567e2ee6b
     */

    @Test
    public void replexTestsAreDisabled() {
        testStatus = "TODO";
    }
}
