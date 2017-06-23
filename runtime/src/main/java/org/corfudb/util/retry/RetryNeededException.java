/**
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package org.corfudb.util.retry;

/**
 * Exception that must be thrown explicitly from the executed function when a retry is needed.
 *
 * <p>Created by Konstantin Spirov on 4/6/2017.
 */
public class RetryNeededException extends Exception {
}
