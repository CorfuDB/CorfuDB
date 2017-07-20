package org.corfudb.util.serializer;

/** Represents an object which the serializer may hash.
 * Created by mwei on 7/16/17.
 */
public interface ICorfuHashable {

    /** Generate a hash for this object to be used by Corfu. */
    byte[] generateCorfuHash();
}
