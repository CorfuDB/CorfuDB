package org.corfudb.protocols.wireprotocol;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;

/**
 * Indicates the priority of client requests.
 *
 * <p>Created by Maithem on 6/24/19.
 */
@AllArgsConstructor
public enum PriorityLevel {
  // This is the default priority for all requests
  NORMAL(0),

  // The priority for clients that need to bypass quota checks (i.e. management clients,
  // checkpointer)
  HIGH(1);

  public final int type;

  public byte asByte() {
    return (byte) type;
  }

  public static final Map<Byte, PriorityLevel> typeMap =
      Arrays.stream(PriorityLevel.values())
          .collect(Collectors.toMap(PriorityLevel::asByte, Function.identity()));
}
