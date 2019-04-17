package org.corfudb.util.metrics;

import java.io.Closeable;
import java.util.concurrent.TimeUnit;

public interface Timer {

    interface Context extends Closeable {
        void stop();
        void close();
    }

    Context getContext();
}
