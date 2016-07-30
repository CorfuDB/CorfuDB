package org.corfudb.infrastructure;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.util.JSONUtils;

import java.util.UUID;

/**
 * Tuple to store the rank and clientId for each round in Paxos.
 * Created by mdhawan on 6/28/16.
 */
@Slf4j
@ToString(exclude = "runtime")
@AllArgsConstructor
public class Rank implements Comparable<Rank> {
    @Getter
    Long rank;
    @Getter
    UUID clientId;

    /**
     * Get a layout from a JSON string.
     */
    public static Rank fromJSONString(String json) {
        return JSONUtils.parser.fromJson(json, Rank.class);
    }

    /**
     * compares this.rank with other.rank
     * if equal
     * compares this.clientId with other.clientId
     *
     * @param other
     * @return
     */
    @Override
    public int compareTo(Rank other) {
        if (rank.compareTo(other.getRank()) == 0) {
            return clientId.compareTo(other.clientId);
        }
        return rank.compareTo(other.getRank());
    }

    /**
     * Get the layout as a JSON string.
     */
    public String asJSONString() {
        return JSONUtils.parser.toJson(this);
    }
}
