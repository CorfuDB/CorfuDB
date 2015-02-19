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

package org.corfudb.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.List;
import java.util.LinkedList;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import java.lang.reflect.Method;
import java.lang.reflect.InvocationTargetException;
import java.lang.StringBuilder;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonArrayBuilder;

import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;

/**
 * This class provides a view of the CorfuDB infrastructure. Clients
 * should not directly access the view without an interface.
 *
 * @author Michael Wei <mwei@cs.ucsd.edu>
 */

public class CorfuDBViewSegment {
    private static final Logger log = LoggerFactory.getLogger(CorfuDBViewSegment.class);

    private List<List<IServerProtocol>> groups;
    private long start;
    private long sealed;

    public CorfuDBViewSegment(long start, long sealed, List<List<IServerProtocol>> groups) {
        this.groups = groups;
        this.sealed = sealed;
        this.start = start;
    }

    public List<List<IServerProtocol>> getGroups() {
        return groups;
    }

    public long getStart() {
        return start;
    }

    public long getSealed() {
        return sealed;
    }
}

