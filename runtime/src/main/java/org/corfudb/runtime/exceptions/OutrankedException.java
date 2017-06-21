package org.corfudb.runtime.exceptions;

import lombok.Getter;
import org.corfudb.runtime.view.Layout;

/**
 * Created by mwei on 12/14/15.
 */
public class OutrankedException extends Exception {
    @Getter
    long newRank;

    @Getter
    Layout layout;

    /**
     * Constructor.
     * @param newRank rank encountered
     */
    public OutrankedException(long newRank) {
        super("Higher rank " + newRank + " encountered, layout = null");
        this.newRank = newRank;
        this.layout = null;
    }

    /**
     * Constructor.
     * @param newRank rank encountered
     * @param layout layout
     */
    public OutrankedException(long newRank, Layout layout) {
        super("Higher rank " + newRank + " encountered, layout = " + ((layout == null)
                ? "null" : layout.toString()));
        this.newRank = newRank;
        this.layout = layout;
    }
}
