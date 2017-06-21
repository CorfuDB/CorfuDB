package org.corfudb.aspects;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.corfudb.runtime.CorfuRuntime;

@Aspect
public class CorfuSmrObjectConcurrencyAspect {

    /** Disable runtime cache */
    @Around("call(org.corfudb.runtime.CorfuRuntime org.corfudb.runtime.view.AbstractViewTest.getDefaultRuntime(..))")
    public CorfuRuntime disableRuntimeCache(ProceedingJoinPoint pjp) {
        try {
            CorfuRuntime rt = (CorfuRuntime) pjp.proceed();
            rt.setCacheDisabled(true);
            return rt;
        } catch (Throwable throwable) {
            throwable.printStackTrace();
            return null;
        }
    }
}
