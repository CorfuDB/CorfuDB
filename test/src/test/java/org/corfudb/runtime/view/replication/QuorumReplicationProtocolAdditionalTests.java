/**
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.corfudb.runtime.view.replication;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.assertj.core.api.Assertions;
import org.corfudb.infrastructure.LogUnitServer;
import org.corfudb.infrastructure.LogUnitServerAssertions;
import org.corfudb.infrastructure.SequencerServer;
import org.corfudb.infrastructure.ServerContextBuilder;
import org.corfudb.infrastructure.TestLayoutBuilder;
import org.corfudb.infrastructure.TestServerRouter;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.IMetadata;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.TokenRequest;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.protocols.wireprotocol.WriteMode;
import org.corfudb.protocols.wireprotocol.WriteRequest;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.stream.IStreamView;
import org.corfudb.util.serializer.Serializers;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertNotNull;

/**
 * Created by Konstantin Spirov on 1/30/2017.
 */
public class QuorumReplicationProtocolAdditionalTests extends AbstractViewTest {


    public static final UUID testClientId = UUID.nameUUIDFromBytes("TEST_CLIENT".getBytes());
    private Layout layout = null;
    private CorfuRuntime corfuRuntime = null;

    @Before
    public void before() {

        addServer(SERVERS.PORT_0);
        addServer(SERVERS.PORT_1);
        addServer(SERVERS.PORT_2);

        layout = new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(SERVERS.PORT_0)
                .addLayoutServer(SERVERS.PORT_1)
                .addLayoutServer(SERVERS.PORT_2)
                .addSequencer(SERVERS.PORT_0)
                .buildSegment()
                .setReplicationMode(Layout.ReplicationMode.QUORUM_REPLICATION)
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addLogUnit(SERVERS.PORT_1)
                .addLogUnit(SERVERS.PORT_2)
                .addToSegment()
                .addToLayout()
                .build();
        bootstrapAllServers(layout);
        getManagementServer(SERVERS.PORT_0).shutdown();
        getManagementServer(SERVERS.PORT_1).shutdown();
        getManagementServer(SERVERS.PORT_2).shutdown();

        layout.getSegment(0L).setReplicationMode(Layout.ReplicationMode.QUORUM_REPLICATION);

        corfuRuntime = new CorfuRuntime();
        corfuRuntime.setCacheDisabled(true);
        layout.getLayoutServers().forEach(corfuRuntime::addLayoutServer);


        layout.getAllServers().forEach(serverEndpoint -> {
            corfuRuntime.getRouter(serverEndpoint).setTimeoutConnect(PARAMETERS.TIMEOUT_VERY_SHORT.toMillis());
            corfuRuntime.getRouter(serverEndpoint).setTimeoutResponse(PARAMETERS.TIMEOUT_VERY_SHORT.toMillis());
            corfuRuntime.getRouter(serverEndpoint).setTimeoutRetry(PARAMETERS.TIMEOUT_VERY_SHORT.toMillis());
        });

        corfuRuntime.connect();

    }


  public CorfuRuntime getDefaultRuntime() {
        return corfuRuntime;
  }

    @Test
    @SuppressWarnings("unchecked")
    public void checkRecoveryWriteTriggeredFromReadRecoversDataWhenTheQuorumIsLost()
            throws Exception {

        //configure the layout accordingly
        CorfuRuntime r = getDefaultRuntime();

        LogUnitServer u0 = getLogUnit(SERVERS.PORT_0);
        LogUnitServer u1 = getLogUnit(SERVERS.PORT_1);
        LogUnitServer u2 = getLogUnit(SERVERS.PORT_2);

        final long ADDRESS_0 = 0L;

        //write at 0
        ByteBuf b = Unpooled.buffer();
        Serializers.CORFU.serialize("0".getBytes(), b);
        WriteRequest m = WriteRequest.builder()
                .writeMode(WriteMode.NORMAL)
                .data(new LogData(DataType.DATA, b))
                .build();
        m.setGlobalAddress(ADDRESS_0);
        m.setRank(new IMetadata.DataRank(0));
        m.setBackpointerMap(Collections.emptyMap());
        sendMessage(u1, CorfuMsgType.WRITE.payloadMsg(m));
        sendMessage(u2, CorfuMsgType.WRITE.payloadMsg(m));
        u2.setShutdown(true);
        u2.shutdown();

        LogUnitServerAssertions.assertThat(u0)
                .isEmptyAtAddress(ADDRESS_0);

        assertThat(r.getAddressSpaceView().read(0L).getPayload(getRuntime()))
                .isEqualTo("0".getBytes());

        LogUnitServerAssertions.assertThat(u1)
                .matchesDataAtAddress(ADDRESS_0, "0".getBytes());

        LogUnitServerAssertions.assertThat(u0)
                .matchesDataAtAddress(ADDRESS_0, "0".getBytes());

    }



    @Test
    @SuppressWarnings("unchecked")
    public void checkReadOnEmptyPosition()
            throws Exception {

        //configure the layout accordingly
        CorfuRuntime r = getDefaultRuntime();

        LogUnitServer u0 = getLogUnit(SERVERS.PORT_0);

        UUID streamA = CorfuRuntime.getStreamID("stream A");

        byte[] testPayload = "hello world".getBytes();


        //generate a stream hole
        TokenResponse tr =
                r.getSequencerView().nextToken(Collections.singleton(streamA), 1);

        IStreamView sv = r.getStreamsView().get(streamA);
        sv.append(testPayload);

        tr = r.getSequencerView().nextToken(Collections.singleton(streamA), 1);

        //make sure we can still read the stream.
        assertThat(sv.next().getPayload(getRuntime()))
                .isEqualTo(testPayload);

        int address = 0;
        assertThat(r.getAddressSpaceView().read(address++).getType()).isEqualTo(DataType.HOLE);
        assertThat(r.getAddressSpaceView().read(address++).getType()).isEqualTo(DataType.DATA);
        assertThat(r.getAddressSpaceView().read(address++).getType()).isEqualTo(DataType.HOLE);

//      TODO(mwei) - fix me
//      assertThat(r.getAddressSpaceView().read(address++).getType()).isEqualTo(DataType.EMPTY);

    }



    @Test
    @SuppressWarnings("unchecked")
    public void canReadWrite()
            throws Exception {
        CorfuRuntime r = getDefaultRuntime();
        UUID streamA = UUID.nameUUIDFromBytes("stream A".getBytes());
        byte[] testPayload = "hello world".getBytes();
        r.getAddressSpaceView().write(new TokenResponse(0,
                        r.getLayoutView().getLayout().getEpoch(),
                        Collections.singletonMap(streamA, Address.NO_BACKPOINTER)),
                testPayload);
        ILogData x = r.getAddressSpaceView().read(0);
        assertNotNull(x.getRank());
        assertThat(r.getAddressSpaceView().read(0L).getPayload(r))
                .isEqualTo("hello world".getBytes());

        assertThat(r.getAddressSpaceView().read(0L).containsStream(streamA))
                .isTrue();

        assertThat((IMetadata.DataRank) r.getAddressSpaceView().read(0L).getMetadataMap()
                .get(IMetadata.LogUnitMetadataType.RANK)).isNotNull();

    }

    @Test
    @SuppressWarnings("unchecked")
    public void canReadWriteConcurrent()
            throws Exception {
        CorfuRuntime r = getDefaultRuntime();

        final int numberThreads = 5;
        final int numberRecords = 1_000;

        scheduleConcurrently(numberThreads, threadNumber -> {
            int base = threadNumber * numberRecords;
            for (int i = base; i < base + numberRecords; i++) {
                r.getAddressSpaceView().write(new TokenResponse((long)i,
                                r.getLayoutView().getLayout().getEpoch(),
                                Collections.singletonMap(CorfuRuntime.getStreamID("a"), Address.NO_BACKPOINTER)),
                        Integer.toString(i).getBytes());
            }
        });
        executeScheduled(numberThreads, PARAMETERS.TIMEOUT_LONG);

        scheduleConcurrently(numberThreads, threadNumber -> {
            int base = threadNumber * numberRecords;
            for (int i = base; i < base + numberRecords; i++) {
                assertThat(r.getAddressSpaceView().read(i).getPayload(getRuntime()))
                        .isEqualTo(Integer.toString(i).getBytes());
            }
        });
        executeScheduled(numberThreads, PARAMETERS.TIMEOUT_LONG);
        assertNotNull(r.getAddressSpaceView().read(0L).getRank());
        assertThat((IMetadata.DataRank) r.getAddressSpaceView().read(0L).getMetadataMap()
                .get(IMetadata.LogUnitMetadataType.RANK)).isNotNull();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void canReadWriteToMultiple()
            throws Exception {


        //configure the layout accordingly
        CorfuRuntime r = getDefaultRuntime();


        UUID streamA = UUID.nameUUIDFromBytes("stream A".getBytes());
        byte[] testPayload = "hello world".getBytes();

        r.getAddressSpaceView().write(new TokenResponse(0,
                        r.getLayoutView().getLayout().getEpoch(),
                        Collections.singletonMap(streamA, Address.NO_BACKPOINTER)),
                testPayload);

        assertThat(r.getAddressSpaceView().read(0L).getPayload(getRuntime()))
                .isEqualTo("hello world".getBytes());

        assertThat(r.getAddressSpaceView().read(0L)
                .containsStream(streamA)).isTrue();
                assertThat((IMetadata.DataRank) r.getAddressSpaceView().read(0L).getMetadataMap()
                .get(IMetadata.LogUnitMetadataType.RANK)).isNotNull();

    }

    @Test
    @SuppressWarnings("unchecked")
    public void ensureAllUnitsContainData()
            throws Exception {


        //configure the layout accordingly
        CorfuRuntime r = getDefaultRuntime();

        UUID streamA = UUID.nameUUIDFromBytes("stream A".getBytes());
        byte[] testPayload = "hello world".getBytes();

        r.getAddressSpaceView().write(new TokenResponse(0, 1,
                        Collections.singletonMap(streamA, Address.NO_BACKPOINTER)),
                testPayload);

        assertThat(r.getAddressSpaceView().read(0L).getPayload(getRuntime()))
                .isEqualTo("hello world".getBytes());

        assertThat(r.getAddressSpaceView().read(0L).containsStream(streamA));

        // Ensure that the data was written to each logunit.
        LogUnitServerAssertions.assertThat(getLogUnit(SERVERS.PORT_0))
                .matchesDataAtAddress(0, testPayload);
        LogUnitServerAssertions.assertThat(getLogUnit(SERVERS.PORT_1))
                .matchesDataAtAddress(0, testPayload);
        LogUnitServerAssertions.assertThat(getLogUnit(SERVERS.PORT_2))
                .matchesDataAtAddress(0, testPayload);
    }



    public void sendMessage(LogUnitServer s, CorfuMsg message) {
        TestServerRouter router = new TestServerRouter();
        router.addServer(s);
        message.setClientID(testClientId);
        message.setRequestID(requestCounter.getAndIncrement());
        router.sendServerMessage(message);
    }

    private AtomicInteger requestCounter = new AtomicInteger(0);

}
