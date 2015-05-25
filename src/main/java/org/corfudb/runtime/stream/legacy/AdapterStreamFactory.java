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
package org.corfudb.runtime.stream.legacy;

import org.corfudb.runtime.CorfuDBRuntime;
import org.corfudb.runtime.smr.IStreamFactory;
import org.corfudb.runtime.stream.IStream;
import org.corfudb.runtime.view.IStreamingSequencer;
import org.corfudb.runtime.view.IWriteOnceAddressSpace;
import java.util.*;


class AdapterStreamFactory implements IStreamFactory {

    IWriteOnceAddressSpace was;
    IStreamingSequencer ss;
    CorfuDBRuntime crt;

    public AdapterStreamFactory(CorfuDBRuntime tcrt, IWriteOnceAddressSpace twas, IStreamingSequencer tss) {
        was = twas;
        ss = tss;
        crt = tcrt;
    }

    public IStream newStream(UUID streamid) { return new AdapterStream(crt, streamid.getMostSignificantBits(), ss, was); }

    public IStream newStream(long streamid) { return new AdapterStream(crt, streamid, ss, was); }
}
