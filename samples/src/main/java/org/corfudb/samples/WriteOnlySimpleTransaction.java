package org.corfudb.samples;

import java.util.Map;

import com.google.common.reflect.TypeToken;
import org.corfudb.runtime.collections.SMRMap;

/**
 * A write-only transaction is a normal transaction that has only object-mutator method invocations,
 * and does not perform any object-accessor invocations.
 *
 * In the default (Optimistic) transaction isolation level, since it has no read-set,
 * a write-only transaction never needs to abort due to another transaction's commit.
 *
 * The main reason to group mutator updates into a write-transaction is commit *atomicity*:
 * Either all of mutator updates are visible to an application or none.
 *
 * This program illustrates the write atomicity concept with a simple transaction example.
 *
 * Created by dalia on 12/30/16.
 */
public class WriteOnlySimpleTransaction extends BaseCorfuAppUtils {
    /**
     * main() and standard setup methods are deferred to BaseCorfuAppUtils
     * @return
     */
    static BaseCorfuAppUtils selfFactory() { return new WriteOnlySimpleTransaction(); }
    public static void main(String[] args) { selfFactory().start(args); }


    /**
     * This is where activity is started
     */
    @Override
    @SuppressWarnings("checkstyle:printLine") // Sample code
    void action() {
        /**
         * Instantiate a Corfu Stream named "A" dedicated to an SMRmap object.
         */
        Map<String, Integer> map = instantiateCorfuObject(
                new TypeToken<SMRMap<String, Integer>>() {}, "A");


        // thread 1: update "a" and "b" atomically
        new Thread(() -> {
            TXBegin();
            map.put("a", 1);
            map.put("b", 1);
            TXEnd();
        }
        ).start();

        // thread 2: read "a", then "b"
        // this thread will print either both 1's, or both nil's
        new Thread(() -> {
            Integer valA = 0, valB = 0;

            TXBegin();
            valA = map.get("a");
            System.out.println("a: " + valA);
            valB = map.get("b");
            System.out.println("b: " + valB);
            TXEnd();
        }
        ).start();
    }
}
