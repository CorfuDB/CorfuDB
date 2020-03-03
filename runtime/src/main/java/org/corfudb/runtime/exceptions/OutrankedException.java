package org.corfudb.runtime.exceptions;

import lombok.Getter;
import org.corfudb.runtime.view.Layout;

import java.util.Comparator;

/**
 * Created by mwei on 12/14/15.
 */
public class OutrankedException extends RuntimeException {
    @Getter
    long newRank;

    @Getter
    Layout layout;

    /**
     * Sorting OutrankedException according to newRanks in descending order
     */
    public static final Comparator<OutrankedException> OUTRANKED_EXCEPTION_COMPARATOR
            = Comparator.comparing(OutrankedException::getNewRank).reversed();

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
