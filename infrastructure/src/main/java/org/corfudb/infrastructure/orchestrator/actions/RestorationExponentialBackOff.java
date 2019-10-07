package org.corfudb.infrastructure.orchestrator.actions;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.result.Result;
import org.corfudb.runtime.exceptions.OutrankedException;
import org.corfudb.runtime.exceptions.QuorumUnreachableException;
import org.corfudb.runtime.exceptions.RetryExhaustedException;
import org.corfudb.runtime.exceptions.WrongEpochException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

@Slf4j
public class RestorationExponentialBackOff {

    private RestorationExponentialBackOff() {

    }

    private static final List<Integer> sequence = Arrays.asList(1, 1, 2, 3, 5, 8, 13);

    private static final List<Class<? extends RuntimeException>> expectedErrors =
            Arrays.asList(WrongEpochException.class,
                    QuorumUnreachableException.class,
                    OutrankedException.class);

    public static boolean execute(Supplier<Boolean> restoreRedundancyAction){
        for (int num : sequence) {

            Result<Boolean, RuntimeException> runResult = Result.of(restoreRedundancyAction);
            if(runResult.isError()){
                System.out.println("Error: " + runResult.getError() + "\n\n\n");
            }
            if (runResult.isError() && expectedErrors.contains(runResult.getError().getClass())) {
                log.error("Handling {}, waiting for {} seconds.", runResult
                        .getError().getMessage(), num);
                doWait(1000 * num);
            }
            else{
                return runResult.get();
            }
        }

        throw new RetryExhaustedException("All retries are exhausted.");
    }


    private static void doWait(int numMillis) {
        try {
            Thread.sleep(numMillis);
        } catch (InterruptedException e) {
            throw new IllegalStateException( e );
        }
    }

}
