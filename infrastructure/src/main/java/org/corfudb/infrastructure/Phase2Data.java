package org.corfudb.infrastructure;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;
import org.corfudb.runtime.view.Layout;

/**
 * Phase2 data consists of rank and the proposed layout.
 * The container class provides a convenience to persist and retrieve
 * these two pieces of data together.
 */
@Data
@ToString
@AllArgsConstructor
public class Phase2Data {
    Rank rank;
    Layout layout;
}