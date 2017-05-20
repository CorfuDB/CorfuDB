package org.corfudb.format.log;

/**
 * Created by mwei on 5/19/17.
 */
public interface ISMREntry {

    /** Return the name of the SMR mutator
     * this entry applies to the object.
     *
     * @return  The name of the mutator to
     *          apply to the object.
     */
    String getSMRMethod();
}
