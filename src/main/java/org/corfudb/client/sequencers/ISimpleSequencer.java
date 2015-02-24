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

package org.corfudb.client.sequencers;
import org.corfudb.client.NetworkException;

/**
 * This interface represents the simplest type of sequencer. Simple sequencers
 * implement only sequenceGetNext(), which returns the next value in the
 * sequence.
 */

public interface ISimpleSequencer {
    long sequenceGetNext() throws NetworkException;
    long sequenceGetCurrent() throws NetworkException;
}

