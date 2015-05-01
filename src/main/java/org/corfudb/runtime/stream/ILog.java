package org.corfudb.runtime.stream;

/**
 * This interface represents an append-only stream. The basic operations of an append-only stream are:
 *
 * append, which places a new entry at the tail of the stream
 * read, which allows for random reads
 * trim, which trims a prefix of the stream, exclusive
 *
 * Created by mwei on 4/30/15.
 */
public interface ILog {
}
