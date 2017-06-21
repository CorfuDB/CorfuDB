package org.corfudb.aspects;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import static org.corfudb.util.CoopScheduler.sched;

/**
 * Attempt to use AspectJ to reproduce the process scheduling
 * interleaving necessary to trigger the problem described by
 * GitHub issue #802.
 */

@Aspect
public class GH802Aspect {

    @After("call(* org.corfudb.runtime.object.VersionLockedObject.aspectFunc(..))")
    public void sched_oC0() {
        System.err.printf("v");
        sched();
    }

    @After("call(* org.corfudb.runtime.object.transactions.OptimisticTransactionalContext.aspectFunc(..))")
    public void sched_oC1() {
        System.err.printf("o");
        sched();
    }

    /* EXPERIMENTATION */

    /*****
    @After("set(* org.corfudb.runtime.object.VersionLockedObject.upcallTargetMap)")
    public void sched_oSA0() {
        System.err.printf("s0");
        sched();
    }

    @After("get(* org.corfudb.runtime.object.VersionLockedObject.upcallTargetMap)")
    public void sched_oSG0() {
        System.err.printf("g0");
        sched();
    }

    @After("set(* org.corfudb.runtime.object.VersionLockedObject.undoRecordFunctionMap)")
    public void sched_oSA1() {
        System.err.printf("s1");
        sched();
    }

    @After("get(* org.corfudb.runtime.object.VersionLockedObject.undoRecordFunctionMap)")
    public void sched_oSG1() {
        System.err.printf("g1");
        sched();
    }

    @After("set(* org.corfudb.runtime.object.VersionLockedObject.undoFunctionMap)")
    public void sched_oSA2() {
        System.err.printf("s2");
        sched();
    }

    @After("get(* org.corfudb.runtime.object.VersionLockedObject.undoFunctionMap)")
    public void sched_oSG2() {
        System.err.printf("g2");
        sched();
    }

    @After("set(* org.corfudb.runtime.object.VersionLockedObject.resetSet)")
    public void sched_oSA3() {
        System.err.printf("s3");
        sched();
    }

    @After("get(* org.corfudb.runtime.object.VersionLockedObject.resetSet)")
    public void sched_oSG3() {
        System.err.printf("g3");
        sched();
    }
    *****/
}
