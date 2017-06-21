package org.corfudb.aspects;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.corfudb.runtime.view.replication.ReadWaitHoleFillPolicy;
import lombok.Getter;
import lombok.Setter;
import static org.corfudb.util.CoopScheduler.sched;

@Aspect
public class CkTrimTestAspect {
    @Getter
    @Setter
    private boolean enabled = false;
    private boolean warned = false;

    /*
     * CheckpointWriter.java's sched() points:
     *   0. before startCheckpoint()
     *   1. before each call to the stream view append().
     *
     */
    @Before("call(* org.corfudb.runtime.CheckpointWriter.startCheckpoint(..))")
    public void schedCpw0(JoinPoint tjp)  {
        ////System.err.printf("startCheckpoint,");
        sched();
    }

    @Before("call(* org.corfudb.runtime.view.StreamsView.append(..))")
    public void schedCpw1(JoinPoint tjp)  {
        ////System.err.printf("appendCP,");
        sched();
    }

    /*
     * LogUnitClient.java's sched() points: before each client RPC call,
     *     write, writeEmptyData, read, getTail, trim, prefixTrim, compact,
     *     flushCache, fillHole,
     */

    @Before("call(* org.corfudb.runtime.clients.LogUnitClient.write(..))")
    public void schedLuc0(JoinPoint tjp)  {
        ////System.err.printf("w,");
        sched();
    }

    @Before("call(* org.corfudb.runtime.clients.LogUnitClient.writeEmptyData(..))")
    public void schedLuc1(JoinPoint tjp)  {
        ////System.err.printf("wE,");
        sched();
    }

    @Before("call(* org.corfudb.runtime.clients.LogUnitClient.read(..))")
    public void schedLuc2(JoinPoint tjp)  {
        ////System.err.printf("r,");
        sched();
    }

    @Before("call(* org.corfudb.runtime.clients.LogUnitClient.getTail(..))")
    public void schedLuc3(JoinPoint tjp)  {
        ////System.err.printf("gT,");
        sched();
    }

    @Before("call(* org.corfudb.runtime.clients.LogUnitClient.trim(..))")
    public void schedLuc4(JoinPoint tjp)  {
        ////System.err.printf("t,");
        sched();
    }

    @Before("call(* org.corfudb.runtime.clients.LogUnitClient.prefixTrim(..))")
    public void schedLuc5(JoinPoint tjp)  {
        ////System.err.printf("pT,");
        sched();
    }

    @Before("call(* org.corfudb.runtime.clients.LogUnitClient.compact(..))")
    public void schedLuc6(JoinPoint tjp)  {
     ////System.err.printf("c,");
     sched();
    }

    @Before("call(* org.corfudb.runtime.clients.LogUnitClient.flushCache(..))")
    public void schedLuc7(JoinPoint tjp)  {
        ////System.err.printf("fC,");
        sched();
    }

    @Before("call(* org.corfudb.runtime.clients.LogUnitClient.fillHole(..))")
    public void schedLuc8(JoinPoint tjp)  {
        ////System.err.printf("fH,");
        sched();
    }

    /*
     * SequencerClient.java's sched() points: before each client RPC call,
     *     nextToken, reset
     */

    @Before("call(* org.corfudb.runtime.clients.SequencerClient.nextToken(..))")
    public void schedSc0(JoinPoint tjp)  {
        ////System.err.printf("Sn,");
        sched();
    }

    @Before("call(* org.corfudb.runtime.clients.SequencerClient.reset(..))")
    public void schedSc1(JoinPoint tjp)  {
        ////System.err.printf("SRESET,");
        sched();
    }

    /* TODO I added these in my initial experiments, but the printf() calls don't fire in this impl.
     * VersionLockedObject.java's sched() points:
     *   0. before optimisticStream.pos()
     *   1. before & after optimisticRollbackUnsafe()
     *   2. before getOptimisticStreamUnsafe()
     *   3. before optimisticCommitUnsafe()
     *   4. before optimisticallyOwnedByThreadUnsafe()
     *   5. before setOptimisticStreamUnsafe()
     *   6. before isOptimisticallyModifiedUnsafe()
     *   7. before resetUnsafe()
     *   8. before optimisticRollbackUnsafe()
     *
     *   10. before syncStreamUnsafe()
     */

    @Before("call(* org.corfudb.runtime.object.transactions.WriteSetSMRStream.pos(..))")
    public void schedVlo0(JoinPoint tjp)  {
        ////System.err.printf("vlo0,");
        sched();
    }

    @Before("call(* org.corfudb.runtime.object.transactions.WriteSetSMRStream.optimisticRollbackUnsafe(..))")
    @After("call(* org.corfudb.runtime.object.transactions.WriteSetSMRStream.optimisticRollbackUnsafe(..))")
    public void schedVlo1(JoinPoint tjp)  {
        ////System.err.printf("vlo1,");
        sched();
    }

    @Before("call(* org.corfudb.runtime.object.transactions.WriteSetSMRStream.getOptimisticStreamUnsafe(..))")
    public void schedVlo2(JoinPoint tjp)  {
        ////System.err.printf("vlo2,");
        sched();
    }

    @Before("call(* org.corfudb.runtime.object.transactions.WriteSetSMRStream.optimisticCommitUnsafe(..))")
    public void schedVlo3(JoinPoint tjp)  {
        ////System.err.printf("vlo3,");
        sched();
    }

    @Before("call(* org.corfudb.runtime.object.transactions.WriteSetSMRStream.optimisticallyOwnedByThreadUnsafe(..))")
    public void schedVlo4(JoinPoint tjp)  {
        ////System.err.printf("vlo4,");
        sched();
    }

    @Before("call(* org.corfudb.runtime.object.transactions.WriteSetSMRStream.setOptimisticStreamUnsafe(..))")
    public void schedVlo5(JoinPoint tjp)  {
        ////System.err.printf("vlo5,");
        sched();
    }

    @Before("call(* org.corfudb.runtime.object.transactions.WriteSetSMRStream.isOptimisticallyModifiedUnsafe(..))")
    public void schedVlo6(JoinPoint tjp)  {
        ////System.err.printf("vlo6,");
        sched();
    }

    @Before("call(* org.corfudb.runtime.object.transactions.WriteSetSMRStream.resetUnsafe(..))")
    public void schedVlo7(JoinPoint tjp)  {
        ////System.err.printf("vlo7,");
        sched();
    }

    @Before("call(* org.corfudb.runtime.object.transactions.WriteSetSMRStream.optimisticRollbackUnsafe(..))")
    public void schedVlo8(JoinPoint tjp)  {
        ////System.err.printf("vlo8,");
        sched();
    }

    @Before("call(* org.corfudb.runtime.object.transactions.WriteSetSMRStream.syncStreamUnsafe(..))")
    public void schedVlo10(JoinPoint tjp)  {
        ////System.err.printf("vlo10,");
        sched();
    }

    /*
     * CoopStampedLock.java's sched() points:
     *   0. before & after tryOptimisticRead()
     *   1. before validate()
     *   2. before tryConvertToWriteLock()
     *   3. before & after writeLock()
     *   4. before unlock()
     */

    @Before("call(* org.corfudb.util.CoopStampedLock.tryOptimisticRead(..))")
    @After("call(* org.corfudb.util.CoopStampedLock.tryOptimisticRead(..))")
    public void schedCsl0(JoinPoint tjp)  {
        ////System.err.printf("csl0,");
        sched();
    }

    @Before("call(* org.corfudb.util.CoopStampedLock.validate(..))")
    public void schedCsl1(JoinPoint tjp)  {
        ////System.err.printf("csl1,");
        sched();
    }

    @Before("call(* org.corfudb.util.CoopStampedLock.tryConvertToWriteLock(..))")
    public void schedCsl2(JoinPoint tjp)  {
        ////System.err.printf("csl2,");
        sched();
    }

    @Before("call(* org.corfudb.util.CoopStampedLock.writeLock(..))")
    @After("call(* org.corfudb.util.CoopStampedLock.writeLock(..))")
    public void schedCsl3(JoinPoint tjp)  {
        ////System.err.printf("csl3,");
        sched();
    }

    @Before("call(* org.corfudb.util.CoopStampedLock.unlock(..))")
    public void schedCsl4(JoinPoint tjp)  {
        ////System.err.printf("csl4,");
        sched();
    }

    /*
     * sleep hack
     */

    @Around("call(* java.lang.Thread.sleep(..))")
    public void substituteSleep(ProceedingJoinPoint pjp) {
        final int HUNDRED = 100;
        final int THOUSAND = 1000;
        final long SEVERAL = 3L;
        Long arg = (Long) pjp.getArgs()[0];
        Long iters;

        if (arg < HUNDRED) {
            iters = 1L;
        } else if (arg < THOUSAND) {
            iters = 2L;
        } else {
            iters = SEVERAL;
        }
        ////System.err.printf("sleepIters(%d),", iters);
        for (int i = 0; i < iters; i++) {
            sched();
        }
    }

    /* TODO I think there's a way around this  setNumRetries via pure AspectJ, but am uncertain about the annotation notation for it */

    @Before("call(* org.corfudb.runtime.view.replication.IHoleFillPolicy.peekUntilHoleFillRequired(..))")
    public void peekRetry1(JoinPoint tjp) throws Throwable {
        ////System.err.printf("peek,");
        if (tjp.getTarget().getClass() == ReadWaitHoleFillPolicy.class) {
            // System.err.printf("peekBefore(%d),", ((ReadWaitHoleFillPolicy) tjp.getTarget()).getNumRetries());
            ((ReadWaitHoleFillPolicy) tjp.getTarget()).setNumRetries(1);
            // System.err.printf("peekYAY(%d),", ((ReadWaitHoleFillPolicy) tjp.getTarget()).getNumRetries());
        }
    }

}
