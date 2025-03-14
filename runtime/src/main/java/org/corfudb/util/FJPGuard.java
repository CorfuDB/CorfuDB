package org.corfudb.util;

import lombok.extern.slf4j.Slf4j;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.reflect.Field;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;


/**
 * This class is used to monitor and protect the FJP ReleaseCount (RC) from
 * leaking to a max value and causing the FJP to stop executing tasks.
 *
 * <p>This class is a workaround for JDK-8330017.
 *
 * <p>See this issue for more details:
 * <a href="https://bugs.openjdk.org/browse/JDK-8330017">JDK-8330017</a>
 */
@Slf4j
public class FJPGuard {

    private static final int RC_RESET_LIMIT = -10_000;

    private static final int RC_RESET_VALUE = -100;

    private static final int RC_SHIFT = 48;

    private static final long NON_RC_MASK = 0x0000FFFFFFFFFFFFL;

    private volatile int lastRc;

    private final ForkJoinPool pool;

    private final ScheduledExecutorService scheduler;

    public FJPGuard(ForkJoinPool pool) {
        this.pool = pool;
        this.scheduler = Executors.newScheduledThreadPool(1, r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            t.setName("FJP-guard");
            return t;
        });
    }

    public void start() {
        scheduler.scheduleWithFixedDelay(
            () -> LambdaUtils.runSansThrow(this::checkAndUpdateCtl),
            60,
            60,
            java.util.concurrent.TimeUnit.SECONDS);
    }

    private void checkAndUpdateCtl() {
        try {
            Field ctlField = pool.getClass().getDeclaredField("ctl");
            ctlField.setAccessible(true);
            VarHandle ctlHandle = MethodHandles.privateLookupIn(ForkJoinPool.class, MethodHandles.lookup())
                    .unreflectVarHandle(ctlField);

            long ctl = (long) ctlHandle.getVolatile(pool);
            log.info(ctlAsBinary(ctl));

            // if the last RC value is less than the reset limit, reset the ctl
            if (lastRc < RC_RESET_LIMIT) {
                long newCtl = ((long)RC_RESET_VALUE << RC_SHIFT) | (ctl & NON_RC_MASK);
                boolean success = ctlHandle.compareAndSet(pool, ctl, newCtl);

                if (success) {
                    log.info("Successfully reset ctl to: {}", ctlAsBinary(newCtl));
                } else {
                    log.info("Failed to reset ctl, will retry in next cycle.");
                }
            }
        } catch (Throwable t) {
            log.error("Failed to access/reset FJP ctl!", t);
        }
    }

    private String ctlAsBinary(long value) {
        // convert and pad zeros
        String binaryCtl = String.format("%64s", Long.toBinaryString(value)).replace(' ', '0');
        String binaryRc = binaryCtl.substring(0, 16);
        String binaryTc = binaryCtl.substring(16, 32);
        String binarySs = binaryCtl.substring(32, 48);
        String binaryId = binaryCtl.substring(48, 64);

        lastRc = binaryToInt(binaryRc);

        return "CTL=(" + value + "), " +
                "RC=(" + prettifyBinary(binaryRc) + ", " + binaryToInt(binaryRc) + "), " +
                "TC=(" + prettifyBinary(binaryTc) + ", " + binaryToInt(binaryTc) + "), " +
                "SS=(" + prettifyBinary(binarySs) + ", " + binaryToInt(binarySs) + "), " +
                "ID=(" + prettifyBinary(binaryId) + ", " + binaryToInt(binaryId) + ")";
    }

    private int binaryToInt(String binary) {
        int decimalValue = Integer.parseInt(binary, 2);

        // For negative value, subtract 2^16
        if (binary.charAt(0) == '1') {
            decimalValue -= (1 << 16);
        }
        return decimalValue;
    }

    // Add a space after 8 bits
    private String prettifyBinary(String binary) {
        return binary.replaceAll("(.{8})", "$1 ");
    }

}
