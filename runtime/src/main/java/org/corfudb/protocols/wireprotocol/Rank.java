package org.corfudb.protocols.wireprotocol;

import java.util.UUID;

import io.netty.buffer.ByteBuf;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

/**
 * Tuple to store the rank and clientId for each round in Paxos.
 * Created by mdhawan on 6/28/16.
 */
@Slf4j
@ToString
@AllArgsConstructor
@EqualsAndHashCode
public class Rank {

    @Getter
    Long rank;
    @Getter
    UUID clientId;

    /**
     * Compares only the ranks. Does not use clientIds in the comparison.
     *
     * @param other rank to compare against
     * @return True if this rank is lower than or equal to other rank,
     *     False otherwise or if other is null
     */
    public boolean lessThanEqualTo(Rank other) {
        if (other == null) {
            return false;
        }
        if (this == other) {
            return true;
        }
        return this.rank.compareTo(other.rank) <= 0;
    }

    public void serialize(ByteBuf buf) {
        buf.writeLong(rank);
        buf.writeLong(clientId.getLeastSignificantBits());
        buf.writeLong(clientId.getMostSignificantBits());
    }

    public static Rank deserialize(ByteBuf buf) {
        long rank = buf.readLong();
        long lsb = buf.readLong();
        long msb = buf.readLong();
        return new Rank(rank, new UUID(msb, lsb));
    }
}
