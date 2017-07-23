package org.corfudb.runtime.view;

/**
 * Created by maithem on 6/20/17.
 */
public class StreamOptions {
    public static StreamOptions DEFAULT = new StreamOptions(false, true);

    public final boolean ignoreTrimmed;
    public final boolean doCache;

    public StreamOptions(boolean ignoreTrimmed, boolean doCache) {
        this.ignoreTrimmed = ignoreTrimmed;
        this.doCache = doCache;
    }

    public static StreamOptionsBuilder builder() {
        return new StreamOptionsBuilder();
    }

    public static class StreamOptionsBuilder {
        private boolean ignoreTrimmed;
        private boolean doCache;

        public StreamOptionsBuilder() {

        }

        public StreamOptionsBuilder ignoreTrimmed(boolean ignore) {
            this.ignoreTrimmed = ignore;
            return this;
        }

        public StreamOptionsBuilder doCache(boolean doCache) {
            this.doCache = doCache;
            return this;
        }

        public StreamOptions build() {
            return new StreamOptions(ignoreTrimmed, doCache);
        }
    }
}
