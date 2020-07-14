package org.corfudb.universe.logging;

import java.nio.file.Path;
import java.nio.file.Paths;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NonNull;

/** Specifies policies to collect logs from docker/vm/processes corfu servers */
@Builder
public class LoggingParams {

  @NonNull private final String testName;

  @Default @Getter private final boolean enabled = false;

  public Path getRelativeServerLogDir() {
    return Paths.get(testName);
  }
}
