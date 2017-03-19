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

    /** Not found constant. Used to indicate that a search for an entry
     * did not result in a entry.
     */
    public static final long NOT_FOUND = -3L;

    /** Optimistic constant. Used to indicate that an update is
     * optimistic.
     */
    public static final long OPTIMISTIC = -4L;
}
