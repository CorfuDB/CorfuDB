package org.corfudb.runtime.exceptions;

/**
 * Thrown when the fast loader encounters an exception.
 *
 * <p>Created by Maithem on 1/17/19.
 */
public class FastObjectLoaderException extends RuntimeException {
  public FastObjectLoaderException(String msg) {
    super(msg);
  }
}
