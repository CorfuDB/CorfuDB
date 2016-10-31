package org.corfudb.infrastructure.log;

import java.util.UUID;

import lombok.Data;
import lombok.RequiredArgsConstructor;

/**
 * Created by mwei on 8/8/16.
 */
@Data
@RequiredArgsConstructor
public class LogAddress {
    final Long address;
    final UUID stream;
}
