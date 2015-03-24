package org.corfudb.client;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;

import com.esotericsoftware.kryo.Kryo;

public class CorfuDBClientThreads {
    public static class CorfuDBForkJoinWorker extends ForkJoinWorkerThread
    {
        private ThreadLocal<Kryo> kryos = new ThreadLocal<Kryo>() {
            protected Kryo initialValue() {
                Kryo kryo = new Kryo();
                return kryo;
            }
        };

        public CorfuDBForkJoinWorker(ForkJoinPool pool)
        {
            super(pool);
        }
    }
}

