package org.corfudb.runtime.exceptions;

/**
 * This Exception is thrown when there is an invalid modification to the layout.
 * Example : Manager attempting to set empty layout, sequencer or logunit segments list.
 *
 * <p>Created by zlokhandwala on 10/12/16.
 */
public class LayoutModificationException extends RuntimeException {

    public LayoutModificationException(String message) {
        super(message);
    }
}
