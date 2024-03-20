package org.corfudb.integration;

import com.google.common.reflect.TypeToken;
import org.corfudb.common.compression.Codec;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuRuntime.CorfuRuntimeParameters.CorfuRuntimeParametersBuilder;
import org.corfudb.runtime.collections.ICorfuTable;
import org.corfudb.runtime.collections.PersistentCorfuTable;
import org.corfudb.runtime.exceptions.AbortCause;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.WriteSizeException;
import org.corfudb.util.TableHelper;
import org.corfudb.util.TableHelper.TableType;
import org.corfudb.util.serializer.Serializers;
import org.junit.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.corfudb.util.TableHelper.openTablePlain;

/**
 * A set integration tests that exercise failure modes related to
 * large writes.
 */

public class LargeWriteIT extends AbstractIT {

    @Test
    public void largeStreamWrite() throws Exception {

        final String streamName = "s1";

        // Start node one and populate it with data
        Process server_1 = new CorfuServerRunner()
                .setHost(DEFAULT_HOST)
                .setPort(DEFAULT_PORT)
                .setSingle(true)
                .runServer();

        final int maxWriteSize = 500;

        // Configure a client with a max write limit
        CorfuRuntimeParametersBuilder paramsBuilder = CorfuRuntime.CorfuRuntimeParameters
                .builder()
                .maxWriteSize(maxWriteSize)
                .codecType(Codec.Type.NONE);

        CorfuRuntime runtime = createRuntime(DEFAULT_ENDPOINT, paramsBuilder);
        final int bufSize = maxWriteSize * 2;

        // Attempt to write a payload that is greater than the configured limit.
        assertThatThrownBy(() -> runtime.getStreamsView()
                .get(CorfuRuntime.getStreamID(streamName))
                .append(new byte[bufSize]))
                .isInstanceOf(WriteSizeException.class);
        shutdownCorfuServer(server_1);
    }

    @ParameterizedTest
    @EnumSource(TableType.class)
    public void largeTransaction(TableType tableType) throws Exception {
        final String tableName = "table1";

        // Start node one and populate it with data
        Process server_1 = new CorfuServerRunner()
                .setHost(DEFAULT_HOST)
                .setPort(DEFAULT_PORT)
                .setSingle(true)
                .runServer();

        final int maxWriteSize = 500;

        // Configure a client with a max write limit
        CorfuRuntimeParametersBuilder paramsBuilder = CorfuRuntime.CorfuRuntimeParameters
                .builder()
                .maxWriteSize(maxWriteSize)
                .codecType(Codec.Type.NONE);

        CorfuRuntime runtime = createRuntime(DEFAULT_ENDPOINT, paramsBuilder);
        byte[] largePayload = new byte[maxWriteSize * 2];

        ICorfuTable<String, byte[]> map = openTablePlain(tableName, runtime, Serializers.JAVA, tableType);

        runtime.getObjectsView().TXBegin();
        map.insert("key1", largePayload);
        boolean aborted = false;
        try {
            runtime.getObjectsView().TXEnd();
        } catch (TransactionAbortedException e) {
            aborted = true;
            assertThat(e.getCause()).isInstanceOf(WriteSizeException.class);
            assertThat(e.getAbortCause()).isEqualTo(AbortCause.SIZE_EXCEEDED);
        }
        assertThat(aborted).isTrue();
        shutdownCorfuServer(server_1);
    }
}
