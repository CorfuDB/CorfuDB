package org.corfudb.infrastructure;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.assertj.core.api.Assertions;
import org.corfudb.infrastructure.log.StreamLogFiles;
import org.corfudb.protocols.wireprotocol.*;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.util.serializer.Serializers;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Collections;

import static org.corfudb.infrastructure.LogUnitServerAssertions.assertThat;

/**
 * Created by Konstantin Spirov on 2/4/16.
 */
public class LogUnitServerQuorumReplicationTest extends AbstractServerTest {
    @Override
    public AbstractServer getDefaultServer() {
        return new LogUnitServer(new ServerContextBuilder().build());
    }


    LogUnitServer s;
    @Before
    public void before() {
        String serviceDir = PARAMETERS.TEST_TEMP_DIR;
        s = new LogUnitServer(new ServerContextBuilder()
                .setLogPath(serviceDir)
                .setMemory(false)
                .build());
        this.router.reset();
        this.router.addServer(s);
    }

    private WriteRequest getWriteRequest(WriteMode writeMode, DataType dataType,  String value, int rank) {
        ByteBuf b = ByteBufAllocator.DEFAULT.buffer();
        Serializers.CORFU.serialize(value.getBytes(), b);
        WriteRequest m = WriteRequest.builder()
                .writeMode(writeMode)
                .data(new LogData(dataType, b))
                .build();
        m.setGlobalAddress(0l);
        // m.setStreams(Collections.singleton(CorfuRuntime.getStreamID("a")));
        m.setStreams(Collections.EMPTY_SET);
        m.setRank(rank);
        m.setBackpointerMap(Collections.emptyMap());
        return m;
    }


    @Test
    public void checkPhase1OurankedException() {
        WriteRequest oldE = getWriteRequest(WriteMode.NORMAL.QUORUM_PHASE1, DataType.DATA_PROPOSED,  "1", 2);
        sendMessage(CorfuMsgType.WRITE.payloadMsg(oldE));
        assertThat(s).containsDataAtAddress(0l);
        assertThat(s).matchesDataAtAddress(0, "1".getBytes());
        WriteRequest newE = getWriteRequest(WriteMode.NORMAL.QUORUM_PHASE1, DataType.DATA_PROPOSED, "2", 1);
        sendMessage(CorfuMsgType.WRITE.payloadMsg(newE));
        assertThat(s).containsDataAtAddress(0l);
        assertThat(s).matchesDataAtAddress(0, "1".getBytes());
        Assertions.assertThat(getLastMessage().getMsgType())
                .isEqualTo(CorfuMsgType.ERROR_RANK);
    }

    @Test
    public void checkPhase1OverwriteException() {
        WriteRequest oldE = getWriteRequest(WriteMode.NORMAL, DataType.DATA,  "1", 0);
        sendMessage(CorfuMsgType.WRITE.payloadMsg(oldE));
        assertThat(s).containsDataAtAddress(0l);
        assertThat(s).matchesDataAtAddress(0, "1".getBytes());
        WriteRequest newE = getWriteRequest(WriteMode.NORMAL.QUORUM_PHASE1, DataType.DATA_PROPOSED, "2", 1);
        sendMessage(CorfuMsgType.WRITE.payloadMsg(newE));
        assertThat(s).containsDataAtAddress(0l);
        assertThat(s).matchesDataAtAddress(0, "1".getBytes());
        Assertions.assertThat(getLastMessage().getMsgType())
                .isEqualTo(CorfuMsgType.ERROR_OVERWRITE);
    }



    @Test
    public void checkPhase1DataWithHigherRank() {
        WriteRequest oldE = getWriteRequest(WriteMode.NORMAL.QUORUM_PHASE1, DataType.DATA_PROPOSED,  "1", 1);
        sendMessage(CorfuMsgType.WRITE.payloadMsg(oldE));
        assertThat(s).containsDataAtAddress(0l);
        assertThat(s).matchesDataAtAddress(0, "1".getBytes());
        WriteRequest newE = getWriteRequest(WriteMode.NORMAL.QUORUM_PHASE1, DataType.DATA_PROPOSED, "2", 2);
        sendMessage(CorfuMsgType.WRITE.payloadMsg(newE));
        assertThat(s).containsDataAtAddress(0l);
        assertThat(s).matchesDataAtAddress(0, "2".getBytes());
    }

    @Test
    public void checkPhase1DataWithTheSameRankAndDifferentValue() {
        WriteRequest oldE = getWriteRequest(WriteMode.NORMAL.QUORUM_PHASE1, DataType.DATA_PROPOSED,  "1", 2);
        sendMessage(CorfuMsgType.WRITE.payloadMsg(oldE));
        assertThat(s).containsDataAtAddress(0l);
        assertThat(s).matchesDataAtAddress(0, "1".getBytes());
        WriteRequest newE = getWriteRequest(WriteMode.NORMAL.QUORUM_PHASE1, DataType.DATA_PROPOSED, "2", 2);
        sendMessage(CorfuMsgType.WRITE.payloadMsg(newE));
        assertThat(s).containsDataAtAddress(0l);
        assertThat(s).matchesDataAtAddress(0, "1".getBytes());
        Assertions.assertThat(getLastMessage().getMsgType())
                .isEqualTo(CorfuMsgType.ERROR_RANK);
    }


    @Test
    public void checkPhase1DataWithTheSameRankAndTheSameValue() {
        WriteRequest oldE = getWriteRequest(WriteMode.NORMAL.QUORUM_PHASE1, DataType.DATA_PROPOSED,  "1", 2);
        sendMessage(CorfuMsgType.WRITE.payloadMsg(oldE));
        assertThat(s).containsDataAtAddress(0l);
        assertThat(s).matchesDataAtAddress(0, "1".getBytes());
        WriteRequest newE = getWriteRequest(WriteMode.NORMAL.QUORUM_PHASE1, DataType.DATA_PROPOSED, "1", 2);
        sendMessage(CorfuMsgType.WRITE.payloadMsg(newE));
        assertThat(s).containsDataAtAddress(0l);
        assertThat(s).matchesDataAtAddress(0, "1".getBytes());
        Assertions.assertThat(getLastMessage().getMsgType())
                .isEqualTo(CorfuMsgType.WRITE_OK);
    }


    @Test
    public void checkPhase2OurankedExceptionByDataInPhase1() {
        WriteRequest oldE = getWriteRequest(WriteMode.NORMAL.QUORUM_PHASE1, DataType.DATA_PROPOSED,  "1", 2);
        sendMessage(CorfuMsgType.WRITE.payloadMsg(oldE));
        assertThat(s).containsDataAtAddress(0l);
        assertThat(s).matchesDataAtAddress(0, "1".getBytes());
        WriteRequest newE = getWriteRequest(WriteMode.NORMAL.QUORUM_PHASE2, DataType.DATA_PROPOSED, "2", 1);
        sendMessage(CorfuMsgType.WRITE.payloadMsg(newE));
        assertThat(s).containsDataAtAddress(0l);
        assertThat(s).matchesDataAtAddress(0, "1".getBytes());
        Assertions.assertThat(getLastMessage().getMsgType())
                .isEqualTo(CorfuMsgType.ERROR_RANK);
    }


    @Test
    public void checkPhase2FromPhase1StraightforwardCommitWithTheSameValueAndRank() {
        WriteRequest oldE = getWriteRequest(WriteMode.NORMAL.QUORUM_PHASE1, DataType.DATA_PROPOSED,  "1", 2);
        sendMessage(CorfuMsgType.WRITE.payloadMsg(oldE));
        assertThat(s).containsDataAtAddress(0l);
        assertThat(s).matchesDataAtAddress(0, "1".getBytes());
        WriteRequest newE = getWriteRequest(WriteMode.NORMAL.QUORUM_PHASE2, DataType.DATA_PROPOSED, "1", 2);
        sendMessage(CorfuMsgType.WRITE.payloadMsg(newE));
        assertThat(s).containsDataAtAddress(0l);
        assertThat(s).matchesDataAtAddress(0, "1".getBytes());
        Assertions.assertThat(getLastMessage().getMsgType())
                .isEqualTo(CorfuMsgType.WRITE_OK);
    }



    @Test
    public void checkPhase2FromPhase1CommitWithSameRankAndDifferentValue() {
        WriteRequest oldE = getWriteRequest(WriteMode.NORMAL.QUORUM_PHASE1, DataType.DATA_PROPOSED,  "1", 2);
        sendMessage(CorfuMsgType.WRITE.payloadMsg(oldE));
        assertThat(s).containsDataAtAddress(0l);
        assertThat(s).matchesDataAtAddress(0, "1".getBytes());
        WriteRequest newE = getWriteRequest(WriteMode.NORMAL.QUORUM_PHASE2, DataType.DATA_PROPOSED, "2", 2);
        sendMessage(CorfuMsgType.WRITE.payloadMsg(newE));
        assertThat(s).containsDataAtAddress(0l);
        assertThat(s).matchesDataAtAddress(0, "1".getBytes());
        Assertions.assertThat(getLastMessage().getMsgType())
                .isEqualTo(CorfuMsgType.ERROR_RANK);
    }

    @Test
    public void checkPhase2FromPhase1WithHigherRank() {
        WriteRequest oldE = getWriteRequest(WriteMode.NORMAL.QUORUM_PHASE1, DataType.DATA_PROPOSED,  "1", 1);
        sendMessage(CorfuMsgType.WRITE.payloadMsg(oldE));
        assertThat(s).containsDataAtAddress(0l);
        assertThat(s).matchesDataAtAddress(0, "1".getBytes());
        WriteRequest newE = getWriteRequest(WriteMode.NORMAL.QUORUM_PHASE2, DataType.DATA_PROPOSED, "2", 2);
        sendMessage(CorfuMsgType.WRITE.payloadMsg(newE));
        assertThat(s).containsDataAtAddress(0l);
        assertThat(s).matchesDataAtAddress(0, "2".getBytes());
        Assertions.assertThat(getLastMessage().getMsgType())
                .isEqualTo(CorfuMsgType.WRITE_OK);
    }


}






