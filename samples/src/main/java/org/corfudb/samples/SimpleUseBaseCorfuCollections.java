package org.corfudb.samples;

import com.google.common.reflect.TypeToken;
import org.corfudb.runtime.collections.FGMap;
import org.corfudb.runtime.collections.ISMRMap;
import org.corfudb.runtime.collections.SMRMap;

/**
 * The Corfu Runtime includes a collection of commodity Corfu objects.
 * These Corfu objects are highly optimized and tested.
 * It is recommended that application builders try to make use of these, before developing new,
 * custom-made object types.
 *
 * This class demonstrates two main types: `SMRmap` and `FGmap`.
 *
 * Created by dmalkhi on 1/5/17.
 */
public class SimpleUseBaseCorfuCollections extends BaseCorfuAppUtils {
    /**
     * main() and standard setup methods are deferred to BaseCorfuAppUtils
     * @return
     */
    static BaseCorfuAppUtils selfFactory() { return new SimpleUseBaseCorfuCollections(); }
    public static void main(String[] args) { selfFactory().start(args); }

    public void action() {
        ISMRMap<Integer, Integer> smrMap = (ISMRMap<Integer, Integer>) instantiateCorfuObject(
                new TypeToken<SMRMap<Integer, Integer>>() {}, "mysmr"
        );

        FGMap<Integer, Integer> fgMap = (FGMap<Integer, Integer>) instantiateCorfuObject(
                new TypeToken<FGMap<Integer, Integer>>() {}, "myfg"
        );

        // populate the SMR Map
        final int SAMPLE_SZ = 10_000;
        for (int i = 0; i < SAMPLE_SZ; i++) {
            smrMap.put(i, i);
        }

        // copy the SMRMap onto the FGMap
        fgMap.putAll(smrMap);

    }
}
