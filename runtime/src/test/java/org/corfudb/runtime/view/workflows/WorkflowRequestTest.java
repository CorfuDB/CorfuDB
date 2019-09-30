package org.corfudb.runtime.view.workflows;
import org.corfudb.runtime.CorfuRuntime;
import org.junit.Test;
import org.mockito.Mockito;
import static org.assertj.core.api.Assertions.assertThat;
import java.time.Duration;

public class WorkflowRequestTest {

    @Test
    public void testPredicate(){
        CorfuRuntime corfuRuntime = Mockito.mock(CorfuRuntime.class);
        String currentNode = "localhost";
        int retry = 1;
        Duration timeOut = Duration.ofSeconds(1);
        Duration pollPeriod = Duration.ofSeconds(1);

        AddNode addNode = new AddNode(currentNode, corfuRuntime, retry,
                timeOut, pollPeriod);

        assertThat(addNode.orchestratorSelector().test(currentNode)).isTrue();

        HealNode healNode = new HealNode(currentNode, corfuRuntime, retry,
                timeOut, pollPeriod);

        assertThat(healNode.orchestratorSelector().test(currentNode)).isTrue();

        RestoreRedundancyMergeSegments restoreRedundancyMergeSegments =
                new RestoreRedundancyMergeSegments(currentNode, corfuRuntime, retry,
                timeOut, pollPeriod);

        assertThat(restoreRedundancyMergeSegments.orchestratorSelector().test(currentNode))
                .isTrue();

        ForceRemoveNode forceRemoveNode = new ForceRemoveNode(currentNode, corfuRuntime, retry,
                timeOut, pollPeriod);

        assertThat(forceRemoveNode.orchestratorSelector().test(currentNode)).isFalse();

        RemoveNode removeNode = new RemoveNode(currentNode, corfuRuntime, retry,
                timeOut, pollPeriod);

        assertThat(removeNode.orchestratorSelector().test(currentNode)).isFalse();

    }
}
