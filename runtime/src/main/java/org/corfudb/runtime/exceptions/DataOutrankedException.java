/**
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License
 * at http://www.apache.org/licenses/LICENSE-2.0
 */

package org.corfudb.runtime.exceptions;

/**
 * Thrown when the log entry cannot be appended as it has lower rank than the other on
 * the same log position.
 * Created by Konstantin Spirov on 3/18/2017.
 */
public class DataOutrankedException extends LogUnitException {
}
