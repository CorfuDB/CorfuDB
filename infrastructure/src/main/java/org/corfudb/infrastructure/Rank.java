package org.corfudb.infrastructure;

import java.util.UUID;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

/**
 * One important property that a rank must maintain is that each proposer should be given a
 * disjoint subset of the epochs. This uniqueness is achieved by storing corresponding clientId.
 *
 * In other words, exclusive epochs are guaranteed by adding the requirement to promises
 * that if the rank from the prepare message rank_msg is equal to the last promised rank rank_pro
 * then the proposer must be the same as the proposer who was previously promised.
 *
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
     * @return True if this rank is lower than the other rank,
     *         False otherwise or if other is null
     */
    public boolean lessThan(Rank other) {
        if (other == null) {
            return false;
        }

        return this.rank.compareTo(other.rank) < 0;
    }

    /**
     * Compares only the ranks. Does not use clientIds in the comparison.
     *
     * @param other rank to compare against
     * @return True if this rank is greater than or equal to the other rank,
     *         False otherwise or if other is null
     */
    public boolean greaterThan(Rank other) {
        return this.rank.compareTo(other.rank) > 0;
    }

    /**
     * Compare must use clientId (PID) during equality comparison.
     *
     * @param other rank to compare against
     * @return True if this rank is greater than or equal to the other rank,
     *         False otherwise or if other is null
     */
    public boolean greaterThanOrEqual(Rank other) {
        if (other == null) {
            return false;
        }

        // Uniqueness guarantee.
        if (this.equals(other)) {
            return true;
        }

        return this.rank.compareTo(other.rank) > 0;
    }
}
