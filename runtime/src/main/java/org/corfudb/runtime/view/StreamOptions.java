package org.corfudb.runtime.view;

/**
 * Created by maithem on 6/20/17.
 */
public class StreamOptions {
    public static StreamOptions DEFAULT = new StreamOptions(false);

    public final boolean ignoreTrimmed;

    public StreamOptions(boolean ignoreTrimmed) {
        this.ignoreTrimmed = ignoreTrimmed;
    }

    public static StreamOptionsBuilder builder() {
        return new StreamOptionsBuilder();
    }

    public static class StreamOptionsBuilder {
        private boolean ignoreTrimmed;

        public StreamOptionsBuilder() {

        }

        public StreamOptionsBuilder ignoreTrimmed(boolean ignore) {
            this.ignoreTrimmed = ignore;
            return this;
        }

        public StreamOptions build() {
            return new StreamOptions(ignoreTrimmed);
        }
    }
}
