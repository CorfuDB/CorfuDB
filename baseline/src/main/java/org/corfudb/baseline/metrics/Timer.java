package org.corfudb.baseline.metrics;

import java.io.Closeable;

public interface Timer {

    interface Context extends Closeable {
        void stop();
        void close();
    }

    Context getContext();
}
