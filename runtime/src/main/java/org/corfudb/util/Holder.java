/**
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package org.corfudb.util;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

/**
 * Holds a reference to a given object
 *
 * <p>Created by Konstantin Spirov on 4/7/2017.
 */
@AllArgsConstructor
public class Holder<T> {
    @Getter
    @Setter
    private T ref;
}
