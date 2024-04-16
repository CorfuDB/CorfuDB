package org.corfudb.runtime.collections.corfutable;

import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuRecord;
import org.corfudb.runtime.collections.table.GenericCorfuTable;
import org.corfudb.runtime.object.VersionedObjectIdentifier;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.corfudb.test.CorfuTableSpec;
import org.corfudb.test.TestSchema.Uuid;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

public class TxnSpec implements CorfuTableSpec<Uuid, CorfuRecord<Uuid, Uuid>> {

    @Override
    public void test(CorfuTableSpecContext<Uuid, CorfuRecord<Uuid, Uuid>> ctx) throws Exception {
        CorfuRuntime rt = ctx.getRt();
        GenericCorfuTable<?, Uuid, CorfuRecord<Uuid, Uuid>> corfuTable = ctx.getCorfuTable();

        Uuid key1 = Uuid.newBuilder().setLsb(1).setMsb(1).build();
        Uuid payload1 = Uuid.newBuilder().setLsb(1).setMsb(1).build();
        Uuid metadata1 = Uuid.newBuilder().setLsb(1).setMsb(1).build();
        CorfuRecord<Uuid, Uuid> value1 = new CorfuRecord<>(payload1, metadata1);

        rt.getObjectsView().TXBegin();

        // Table should be empty
        assertThat(corfuTable.get(key1)).isNull();
        assertThat(corfuTable.size()).isZero();

        // Put key1
        corfuTable.insert(key1, value1);

        // Table should now have size 1 and contain key1
        assertThat(corfuTable.get(key1).getPayload().getLsb()).isEqualTo(payload1.getLsb());
        assertThat(corfuTable.get(key1).getPayload().getMsb()).isEqualTo(payload1.getMsb());
        assertThat(corfuTable.size()).isEqualTo(1);
        rt.getObjectsView().TXEnd();

        Uuid key2 = Uuid.newBuilder().setLsb(2).setMsb(2).build();
        Uuid payload2 = Uuid.newBuilder().setLsb(2).setMsb(2).build();
        Uuid metadata2 = Uuid.newBuilder().setLsb(2).setMsb(2).build();
        CorfuRecord<Uuid, Uuid> value2 = new CorfuRecord<>(payload2, metadata2);

        Uuid nonExistingKey = Uuid.newBuilder().setLsb(3).setMsb(3).build();

        rt.getObjectsView().TXBegin();

        // Put key2
        corfuTable.insert(key2, value2);

        // Table should contain both key1 and key2, but not nonExistingKey
        assertThat(corfuTable.get(key2).getPayload().getLsb()).isEqualTo(payload2.getLsb());
        assertThat(corfuTable.get(key2).getPayload().getMsb()).isEqualTo(payload2.getMsb());
        assertThat(corfuTable.get(key1).getPayload().getLsb()).isEqualTo(payload1.getLsb());
        assertThat(corfuTable.get(key1).getPayload().getMsb()).isEqualTo(payload1.getMsb());
        assertThat(corfuTable.get(nonExistingKey)).isNull();
        rt.getObjectsView().TXEnd();

        // Verify the state of the table @ SEQ 0
        rt.getObjectsView().TXBuild()
                .type(TransactionType.SNAPSHOT)
                .snapshot(new Token(0, 0))
                .build()
                .begin();

        assertThat(corfuTable.get(key1).getPayload().getLsb()).isEqualTo(payload1.getLsb());
        assertThat(corfuTable.get(key1).getPayload().getMsb()).isEqualTo(payload1.getMsb());
        assertThat(corfuTable.get(nonExistingKey)).isNull();
        assertThat(corfuTable.get(key2)).isNull();
        assertThat(corfuTable.size()).isEqualTo(1);
        rt.getObjectsView().TXEnd();

        // Verify the state of the table @ SEQ 1
        rt.getObjectsView().TXBuild()
                .type(TransactionType.SNAPSHOT)
                .snapshot(new Token(0, 1))
                .build()
                .begin();

        assertThat(corfuTable.get(key2).getPayload().getLsb()).isEqualTo(payload2.getLsb());
        assertThat(corfuTable.get(key2).getPayload().getMsb()).isEqualTo(payload2.getMsb());
        assertThat(corfuTable.get(key1).getPayload().getLsb()).isEqualTo(payload1.getLsb());
        assertThat(corfuTable.get(key1).getPayload().getMsb()).isEqualTo(payload1.getMsb());
        assertThat(corfuTable.get(nonExistingKey)).isNull();
        assertThat(corfuTable.size()).isEqualTo(2);
        rt.getObjectsView().TXEnd();

        // Verify the MVOCache has exactly 2 versions
        Set<VersionedObjectIdentifier> voIds = rt.getObjectsView().getMvoCache().keySet();
        assertThat(voIds).containsExactlyInAnyOrder(
                new VersionedObjectIdentifier(corfuTable.getCorfuSMRProxy().getStreamID(), -1L),
                new VersionedObjectIdentifier(corfuTable.getCorfuSMRProxy().getStreamID(), 0L));
    }
}
