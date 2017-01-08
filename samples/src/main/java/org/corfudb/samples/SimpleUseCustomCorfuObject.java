package org.corfudb.samples;

/**
 * A simple program
 * that makes use of {@link CorfuSharedCounter} and {@link CorfuCompoundObject}.
 */
public class SimpleUseCustomCorfuObject extends CorfuAppUtils {
    /**
     * main() and standard setup methods are deferred to CorfuAppUtils
     * @return
     */
    static CorfuAppUtils selfFactory() { return new SimpleUseCustomCorfuObject(); }
    public static void main(String[] args) { selfFactory().start(args); }

    public void action() {
        CorfuSharedCounter cntr = instantiateCorfuObject(
                CorfuSharedCounter.class, "CNTR"
        );

        final int MAGIC_VALUE = 55;
        System.out.println("Counter value before increment: " + cntr.Get());
        cntr.Set(MAGIC_VALUE);
        System.out.println("Counter value before increment: " + cntr.Increment());
        System.out.println("Counter value before increment: " + cntr.Increment());

        CorfuCompoundObject cmpnd = instantiateCorfuObject(
                CorfuCompoundObject.class, "CMPND"
        );

        cmpnd.set(cmpnd.new Inner("foo", "bar"), MAGIC_VALUE);
        System.out.println("compound: "
                + "user=" + cmpnd.getUser().getFirstName() + "," + cmpnd.getUser().getLastName()
                + " ID=" + cmpnd.getID());
    }
}
