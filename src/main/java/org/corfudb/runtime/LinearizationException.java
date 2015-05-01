/**
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.corfudb.runtime;

import org.corfudb.runtime.stream.Timestamp;

/**
 * This exception is thrown when a linearization issue occurs (typically
 * when a non-linearizable timestamp is presented for comparison)
 */
@SuppressWarnings("serial")
public class LinearizationException extends Exception
{
    Timestamp ts1;
    Timestamp ts2;
    public LinearizationException(String desc, Timestamp ts1, Timestamp ts2)
    {
        super(desc + "[ts1=" + ts1.toString() + ", ts2=" + ts2.toString() + "]");
        this.ts1 = ts1;
        this.ts2 = ts2;
    }
}

