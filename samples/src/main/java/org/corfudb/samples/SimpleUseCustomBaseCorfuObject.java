package org.corfudb.samples;

import com.google.common.reflect.TypeToken;

/**
 * A simple program
 * that makes use of {@link CorfuSharedCounter} and {@link CorfuCompoundObject}.
 */
public class SimpleUseCustomBaseCorfuObject extends BaseCorfuAppUtils {
    /**
     * main() and standard setup methods are deferred to BaseCorfuAppUtils
     * @return
     */
    static BaseCorfuAppUtils selfFactory() { return new SimpleUseCustomBaseCorfuObject(); }
    public static void main(String[] args) { selfFactory().start(args); }

    @SuppressWarnings("checkstyle:printLine") // Sample code
    public void action() {
        CorfuSharedCounter cntr = instantiateCorfuObject(
                new TypeToken<CorfuSharedCounter>() {}, "CNTR"
        );

        final int MAGIC_VALUE = 55;
        System.out.println("Counter value before increment: " + cntr.Get());
        cntr.Set(MAGIC_VALUE);
        System.out.println("Counter value before increment: " + cntr.Increment());
        System.out.println("Counter value before increment: " + cntr.Increment());

        CorfuCompoundObject cmpnd = instantiateCorfuObject(
                new TypeToken<CorfuCompoundObject>() {}, "CMPND"
        );

        cmpnd.set(cmpnd.new Inner("foo", "bar"), MAGIC_VALUE);
        System.out.println("compound: "
                + "user=" + cmpnd.getUser().getFirstName() + "," + cmpnd.getUser().getLastName()
                + " ID=" + cmpnd.getID());
    }
}
