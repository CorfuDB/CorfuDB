package org.corfudb.samples;

import com.google.common.reflect.TypeToken;
import java.util.Map;
import org.corfudb.runtime.collections.CorfuTable;

/**
 * Consider again the code from {@link org.corfudb.samples::HeloCorfu.java}:
 *
 * <p>Integer previous = map.get("a"); if (previous == null) { System.out.println("This is the first
 * time we were run!"); map.put("a", 1); } else { map.put("a", ++previous); System.out.println("This
 * is the " + previous + " time we were run!"); }
 *
 * <p>If three program instances arrive at this code piece one after another, then the outcome would
 * be: "This is the first time we were run!"<br>
 * "... 2 time ..." <br>
 * "... 3 time"
 *
 * <p>What if we were to execute the above code concurrently? Try it.
 *
 * <p>Note that there is no need to worry about the atomicity of interleaved calls to individual
 * Corfu-object methods, e.g., `map.put` and `map.get`. All Corfu-object methods are guaranteed to
 * be atomic against multi-threaded programs and concurrent program instances.
 *
 * <p>However, there is no guarantee about the interleaving of `map.get` followed by `map.put`, when
 * invoked from concurrent program instances. Therefore, any of the following outputs are valid:
 *
 * <p>Output 1: "... first time ..."<br>
 * "... first time ..."<br>
 * "... first time ..."
 *
 * <p>Output 2: "... first time ..." <br>
 * "... first time ..."<br>
 * "... 2 time ..."
 *
 * <p>Output 3: "... first time ..."<br>
 * "... 2 time ..."<br>
 * "... 2 time ..."
 *
 * <p>Output 4: "... first time ..."<br>
 * "... 2 time ..."<br>
 * "... 3 time ..."
 *
 * <p>In order to enforce a consistent behavior, Corfu provides support for ACID transactions. A
 * transaction is a body of code wrapped with `TXBegin()` and `TXEnd()`. For example, in the code
 * below, we will wrap the code above with a transaction block:
 *
 * <p>corfuRuntime.getObjectsView().TXBegin(); Integer previous = map.get("a"); if (previous ==
 * null) { System.out.println("This is the first time we were run!"); map.put("a", 1); } else {
 * map.put("a", ++previous); System.out.println("This is the " + previous + " time we were run!"); }
 * corfuRuntime.getObjectsView.TXEnd();
 *
 * <p>If we were to run three instances of this program concurrently, we would guarantee a
 * **serializable** execution order. The only valid output would be
 *
 * <p>"... first time ..." <br>
 * "... 2 time ..." <br>
 * "... 3 time ..."
 *
 * <p>Created by dalia on 12/30/16.
 */
public class SimpleAtomicTransaction extends BaseCorfuAppUtils {
  /**
   * main() and standard setup methods are deferred to BaseCorfuAppUtils
   *
   * @return
   */
  static BaseCorfuAppUtils selfFactory() {
    return new SimpleAtomicTransaction();
  }

  public static void main(String[] args) {
    selfFactory().start(args);
  }

  /** this method initiates activity */
  @SuppressWarnings("checkstyle:printLine") // Sample code
  @Override
  void action() {
    /**
     * A Corfu Stream is a log dedicated specifically to the history of updates of one object. We
     * will instantiate a stream by giving it a name "A", and then instantiate an object by
     * specifying its class
     */
    Map<String, Integer> map =
        getCorfuRuntime()
            .getObjectsView()
            .build()
            .setStreamName("A") // stream name
            .setTypeToken(new TypeToken<CorfuTable<String, Integer>>() {})
            .open(); // instantiate the object!

    /**
     * Execute an atomic transaction over the shared map. Inside the transaction, an application
     * first obtains the current value of an entry "a", then it increments the value and replaces
     * the map entry with the incremented value.
     */
    getCorfuRuntime().getObjectsView().TXBegin();
    Integer previous = map.get("a");
    if (previous == null) {
      System.out.println("This is the first time we were run!");
      map.put("a", 1);
    } else {
      map.put("a", ++previous);
      System.out.println("This is the " + previous + " time we were run!");
    }
    getCorfuRuntime().getObjectsView().TXEnd();

    /**
     * If we were to run three instances of this program concurrently, we would guarantee a
     * **serializable** execution order. The only valid output would be "... first time ..." <br>
     * "... 2 time ..." <br>
     * "... 3 time ..."
     */
  }
}
