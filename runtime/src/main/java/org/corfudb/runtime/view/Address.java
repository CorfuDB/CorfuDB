package org.corfudb.runtime.view;

/** Currently, this class holds static constants used
 * to represent addresses. In the future, it may replace the
 * use of long depending on performance.
 *
 * Created by mwei on 1/6/17.
 */
public class Address {

    /** The maximum address. */
    public static final long MAX = Long.MAX_VALUE;

    /** Never read constant. Used by stream implementations to indicate
     * that no entries have ever been returned. */
    public static final long NEVER_READ = -1L;

    /** Aborted request constant. Used to indicate an attempted read, but
     * was rejected at the request of the client.
     */
    public static final long ABORTED = -2L;
}
