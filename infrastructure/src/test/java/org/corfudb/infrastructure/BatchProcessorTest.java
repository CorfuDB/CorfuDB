package org.corfudb.infrastructure;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.BatchProcessor.BatchProcessorContext;
import org.corfudb.infrastructure.BatchProcessor.ExitManager;
import org.corfudb.infrastructure.log.StreamLog;
import org.corfudb.protocols.service.CorfuProtocolMessage.ClusterIdCheck;
import org.corfudb.protocols.service.CorfuProtocolMessage.EpochCheck;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.StreamsAddressResponse;
import org.corfudb.protocols.wireprotocol.TailsResponse;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.exceptions.QuotaExceededException;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.corfudb.runtime.proto.FileSystemStats.BatchProcessorStatus;
import org.corfudb.runtime.proto.service.LogUnit;
import org.corfudb.util.serializer.Serializers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.protocols.CorfuProtocolCommon.DEFAULT_UUID;
import static org.corfudb.protocols.CorfuProtocolCommon.getUuidMsg;
import static org.corfudb.protocols.service.CorfuProtocolBase.getSealRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolLogUnit.getLogAddressSpaceRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolLogUnit.getRangeWriteLogRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolLogUnit.getResetLogUnitRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolLogUnit.getTailRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolLogUnit.getTrimLogRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolLogUnit.getWriteLogRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getHeaderMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getRequestMsg;
import static org.corfudb.runtime.proto.service.CorfuMessage.HeaderMsg;
import static org.corfudb.runtime.proto.service.CorfuMessage.PriorityLevel;
import static org.corfudb.runtime.proto.service.CorfuMessage.RequestMsg;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Slf4j
public class BatchProcessorTest {
    private static final BatchProcessorStatus BP_STATUS_OK = BatchProcessorStatus.BP_STATUS_OK;

    @Rule
    public MockitoRule mockito = MockitoJUnit.rule();

    private BatchProcessor batchProcessor;
    private BatchProcessorContext bpContext;

    private StreamLog mockStreamLog;
    private ExitManager mockExitManager;
    private final AtomicInteger requestCounter = new AtomicInteger();
    private final long DEFAULT_SEAL_EPOCH = 1L;
    private final long LARGER_SEAL_EPOCH = 5L;

    /**
     * A helper method that creates a basic message header populated
     * with default values. Note that the sealEpoch in BatchProcessor
     * should be equal to the epoch in request header.
     *
     * @return the corresponding HeaderMsg
     */
    private HeaderMsg getBasicHeader(ClusterIdCheck ignoreClusterId, EpochCheck ignoreEpoch) {
        return getHeaderMsg(requestCounter.incrementAndGet(), PriorityLevel.NORMAL, DEFAULT_SEAL_EPOCH,
                getUuidMsg(DEFAULT_UUID), getUuidMsg(DEFAULT_UUID), ignoreClusterId, ignoreEpoch);
    }

    /**
     * A helper method that creates a basic message header populated
     * with default values. Note that the sealEpoch in BatchProcessor
     * should be equal to the epoch in request header.
     *
     * @return the corresponding HeaderMsg
     */
    private HeaderMsg getHeaderHighPriority(ClusterIdCheck ignoreClusterId, EpochCheck ignoreEpoch) {
        return getHeaderMsg(requestCounter.incrementAndGet(), PriorityLevel.HIGH, DEFAULT_SEAL_EPOCH,
                getUuidMsg(DEFAULT_UUID), getUuidMsg(DEFAULT_UUID), ignoreClusterId, ignoreEpoch);
    }

    /**
     * A helper method that creates a message header populated with
     * default values and larger epoch than default header. Note that
     * this header is only used to construct RESET request, which is just
     * took for convenience in some exceptional cases.
     *
     * @return the corresponding HeaderMsg
     */
    private HeaderMsg getResetHeaderLargerEpoch() {
        return getHeaderMsg(requestCounter.incrementAndGet(), PriorityLevel.NORMAL, LARGER_SEAL_EPOCH,
                getUuidMsg(DEFAULT_UUID), getUuidMsg(DEFAULT_UUID), ClusterIdCheck.CHECK, EpochCheck.IGNORE);
    }

    /**
     * A helper method that creates a sample LogData object with default values.
     *
     * @param address LogData's global address (global tail)
     * @return the corresponding HeaderMsg
     */
    private LogData getDefaultLogData(long address) {
        ByteBuf b = Unpooled.buffer();
        byte[] streamEntry = "Payload".getBytes();
        Serializers.CORFU.serialize(streamEntry, b);
        LogData ld = new LogData(DataType.DATA, b);
        ld.setGlobalAddress(address);
        ld.setEpoch(0L);
        return ld;
    }

    /**
     * Perform the required preparation before running individual tests.
     */
    @Before
    public void setup() {
        mockStreamLog = mock(StreamLog.class);
        mockExitManager = mock(ExitManager.class);
        bpContext = spy(new BatchProcessorContext());
        batchProcessor = new BatchProcessor(mockStreamLog, bpContext, mockExitManager, DEFAULT_SEAL_EPOCH, true);
    }

    /**
     * Test that the BatchProcessor successfully handles a PREFIX_TRIM request.
     */
    @Test
    public void testPrefixTrim() {
        long epoch = 0L;
        long sequence = 5L;
        RequestMsg request = getRequestMsg(getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.CHECK),
                getTrimLogRequestMsg(new Token(epoch, sequence)));

        batchProcessor.addTask(BatchWriterOperation.Type.PREFIX_TRIM, request).join();
        assertEquals(BP_STATUS_OK, bpContext.getStatus());
        verify(mockStreamLog).prefixTrim(sequence);
    }

    /**
     * Test that the BatchProcessor successfully handles a WRITE request.
     */
    @Test
    public void testWrite() {
        LogData logData = getDefaultLogData(0L);
        RequestMsg request = getRequestMsg(getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.CHECK),
                getWriteLogRequestMsg(logData));

        batchProcessor.addTask(BatchWriterOperation.Type.WRITE, request).join();
        assertEquals(BP_STATUS_OK, bpContext.getStatus());
        verify(mockStreamLog).append(0L, logData);
    }

    /**
     * Test that the BatchProcessor successfully handles a RANGE_WRITE request.
     */
    @Test
    public void testRangeWrite() {
        final int numIter = 100;
        List<LogData> entries = new ArrayList<>();
        for (int x = 0; x < numIter; x++) {
            LogData ld = getDefaultLogData(x);
            entries.add(ld);
        }

        RequestMsg request = getRequestMsg(getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.CHECK),
                getRangeWriteLogRequestMsg(entries));
        batchProcessor.addTask(BatchWriterOperation.Type.RANGE_WRITE, request).join();
        assertEquals(BP_STATUS_OK, bpContext.getStatus());
        verify(mockStreamLog).append(entries);
    }

    /**
     * Test that the BatchProcessor successfully handles a RESET request.
     */
    @Test
    public void testReset() {
        long epochWaterMark = 100L;
        RequestMsg request = getRequestMsg(getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.IGNORE),
                getResetLogUnitRequestMsg(epochWaterMark));
        batchProcessor.addTask(BatchWriterOperation.Type.RESET, request).join();
        assertEquals(BP_STATUS_OK, bpContext.getStatus());
        verify(mockStreamLog).reset();
    }

    /**
     * Test that the BatchProcessor successfully handles a TAILS_QUERY request for LOG_TAIL.
     */
    @Test
    public void testTailsQueryLogTail() {
        RequestMsg request = getRequestMsg(getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.CHECK),
                getTailRequestMsg(LogUnit.TailRequestMsg.Type.LOG_TAIL));

        Object ret = batchProcessor.addTask(BatchWriterOperation.Type.TAILS_QUERY, request).join();
        assertEquals(BP_STATUS_OK, bpContext.getStatus());
        verify(mockStreamLog).getLogTail();
        assertThat(ret).isInstanceOf(TailsResponse.class);
    }

    /**
     * Test that the BatchProcessor successfully handles a TAILS_QUERY request for ALL_STREAMS_TAIL.
     */
    @Test
    public void testTailsQueryAllStreamsTail() {
        RequestMsg request = getRequestMsg(getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.CHECK),
                getTailRequestMsg(LogUnit.TailRequestMsg.Type.ALL_STREAMS_TAIL));

        when(mockStreamLog.getAllTails()).thenReturn(new TailsResponse(0L));
        Object ret = batchProcessor.addTask(BatchWriterOperation.Type.TAILS_QUERY, request).join();
        assertEquals(BP_STATUS_OK, bpContext.getStatus());
        verify(mockStreamLog).getAllTails();
        assertThat(ret).isInstanceOf(TailsResponse.class);
    }

    /**
     * Test that the BatchProcessor successfully handles a LOG_ADDRESS_SPACE_QUERY request.
     */
    @Test
    public void testLogAddressSpaceQuery() {
        RequestMsg request = getRequestMsg(getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.CHECK),
                getLogAddressSpaceRequestMsg());

        when(mockStreamLog.getStreamsAddressSpace()).thenReturn(new StreamsAddressResponse(0L, new HashMap<>()));
        Object ret = batchProcessor.addTask(BatchWriterOperation.Type.LOG_ADDRESS_SPACE_QUERY, request).join();
        verify(mockStreamLog).getStreamsAddressSpace();
        assertThat(ret).isInstanceOf(StreamsAddressResponse.class);
        assertEquals(DEFAULT_SEAL_EPOCH, ((StreamsAddressResponse) ret).getEpoch());
        assertEquals(0L, ((StreamsAddressResponse) ret).getLogTail());
        assertThat(((StreamsAddressResponse) ret).getAddressMap()).isEmpty();
        assertEquals(BP_STATUS_OK, bpContext.getStatus());
    }


    /**
     * Test that the BatchProcessor throws a WrongEpochException when the
     * request epoch doesn't match the sealEpoch.
     */
    @Test(expected = WrongEpochException.class)
    public void testWrongEpoch() throws Throwable {
        RequestMsg request = getRequestMsg(getResetHeaderLargerEpoch(), getResetLogUnitRequestMsg(100L));

        try {
            batchProcessor.addTask(BatchWriterOperation.Type.WRITE, request).join();
            assertEquals(BP_STATUS_OK, bpContext.getStatus());
        } catch (CompletionException e) {
            throw e.getCause();
        }
    }

    /**
     * Test that the BatchProcessor successfully handles a SEAL request.
     */
    @Test(expected = WrongEpochException.class)
    public void testSeal() throws Throwable {
        RequestMsg request = getRequestMsg(getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.IGNORE),
                getSealRequestMsg(LARGER_SEAL_EPOCH));

        batchProcessor.addTask(BatchWriterOperation.Type.SEAL, request);
        // There isn't a getter for sealEpoch of BatchProcessor, so here we create
        // two RESET request, one with default epoch (old) and one with large epoch.
        // The former one should throw an exception and the latter one should succeed.
        RequestMsg badRequest = getRequestMsg(getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.IGNORE),
                getResetLogUnitRequestMsg(100L));
        RequestMsg goodRequest = getRequestMsg(getResetHeaderLargerEpoch(), getResetLogUnitRequestMsg(100L));
        batchProcessor.addTask(BatchWriterOperation.Type.RESET, goodRequest).join();
        verify(mockStreamLog).reset();
        try {
            batchProcessor.addTask(BatchWriterOperation.Type.RESET, badRequest).join();
            assertEquals(BP_STATUS_OK, bpContext.getStatus());
        } catch (CompletionException e) {
            throw e.getCause();
        }
    }

    /**
     * Test that the BatchProcessor throws a QuotaExceededException when its
     * quota limit is reached and successfully handles a RESET request with high priority.
     */
    @Test(expected = QuotaExceededException.class)
    public void testQuotaExceeded() throws Throwable {
        long epochWaterMark = 100L;
        RequestMsg badRequest = getRequestMsg(getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.IGNORE),
                getResetLogUnitRequestMsg(epochWaterMark));
        RequestMsg goodRequest = getRequestMsg(getHeaderHighPriority(ClusterIdCheck.CHECK, EpochCheck.IGNORE),
                getResetLogUnitRequestMsg(epochWaterMark));

        when(mockStreamLog.quotaExceeded()).thenReturn(true);
        batchProcessor.addTask(BatchWriterOperation.Type.RESET, goodRequest).join();
        assertEquals(BP_STATUS_OK, bpContext.getStatus());
        verify(mockStreamLog).reset();
        try {
            batchProcessor.addTask(BatchWriterOperation.Type.RESET, badRequest).join();
            assertEquals(BP_STATUS_OK, bpContext.getStatus());
        } catch (CompletionException e) {
            throw e.getCause();
        }
    }

    /**
     * This test validates that System.exit() is invoked when an unrecoverable
     * exception is throw in the BatchProcessor thread.
     */
    @Test
    public void testBpExceptionExit() throws Exception {
        final AtomicBoolean validateChecked = new AtomicBoolean(false);
        final CountDownLatch latch = new CountDownLatch(1);

        // Mock an IOException from the StreamLogFiles when fsync occurs.
        doThrow(new IOException()).when(mockStreamLog).sync(true);
        doAnswer(invocation -> {
            validateChecked.set(true);
            latch.countDown();
            return null;
        }).when(mockExitManager).exit(CorfuServer.EXIT_ERROR_CODE);

        batchProcessor.addTask(BatchWriterOperation.Type.SHUTDOWN, RequestMsg.getDefaultInstance());
        latch.await();

        assertThat(validateChecked).isTrue();
    }
}
