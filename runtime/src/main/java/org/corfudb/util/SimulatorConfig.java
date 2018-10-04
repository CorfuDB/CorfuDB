package org.corfudb.util;

import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class SimulatorConfig {
    int numThreads;
    int burstPeriod;
    int maxWriteSize;
    int minWriteSize;
    int medianWriteSize;
    int stdWriteSize;
    int maxTxnPerPeriod;
    int minTxnPerPeriod;
    int medianTxnPerPeriod;
    int stdTxnPerPeriod;
    int numTables;
    int maxTablesPerTxn;
    int minTablesPerTxn;
    int medianTablesPerTxn;
    int stdTablesPerTxn;
    int maxKeysPerTxn;
    int minKeysPerTxn;
    int stdKeysPerTxn;
    int medianKeysPerTxn;
    int minKeySize;
    int stdKeySize;
    int medianKeySize;
}