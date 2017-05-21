package org.corfudb.annotations;

/** A direct read function supports reading an update from
 * a single SMREntry, without reading the entire history
 * of the object.
 *
 * Created by mwei on 5/19/17.
 */
public @interface DirectReadFunction {

    /** The name of the mutator this direct
     * read function handles.
     * @return  The name of the mutator.
     * */
    String mutatorName() default "";
}
