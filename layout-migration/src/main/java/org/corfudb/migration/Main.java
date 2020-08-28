package org.corfudb.migration;

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.UUID;
import org.corfudb.runtime.exceptions.DataCorruptionException;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.JsonUtils;

public class Main {

  public static void main(String[] argv) throws Exception {
    if (argv.length == 0) {
      throw new IllegalArgumentException("LAYOUT_CURRENT file path not specified!");
    }
    String layoutPath = argv[0];
    UUID clusterId;
    if (argv.length < 2) {
      clusterId = UUID.randomUUID();
    } else {
      clusterId = UUID.fromString(argv[1]);
    }

    Path path = Paths.get(layoutPath);
    if (Files.notExists(path)) {
      throw new IllegalArgumentException("invalid path :" + layoutPath);
    }

    boolean dsExtension = layoutPath.endsWith(".ds");

    byte[] bytes = Files.readAllBytes(path);
    ByteBuffer buf = ByteBuffer.wrap(bytes);
    int checksum = buf.getInt();
    int skipBytes = dsExtension ? 4 : 0;
    byte[] strBytes = Arrays.copyOfRange(bytes, skipBytes, bytes.length);
    if (dsExtension && checksum != getChecksum(strBytes)) {
      throw new DataCorruptionException();
    }

    String json = new String(strBytes);
    Layout newLayout = JsonUtils.parser.fromJson(json, Layout.class);

    if (newLayout.getClusterId() != null) {
      System.out.println("Current cluster id " + newLayout.getClusterId());
    } else {
      System.out.println("generating random cluster id " + clusterId);
      newLayout.setClusterId(clusterId);
    }

    String newJsonPayload = JsonUtils.parser.toJson(newLayout, Layout.class);
    byte[] newLayoutBytes = newJsonPayload.getBytes();
    int checksumBytes = dsExtension ? Integer.BYTES : 0;
    ByteBuffer buffer = ByteBuffer.allocate(newLayoutBytes.length
            + checksumBytes);
    if (dsExtension) {
      buffer.putInt(getChecksum(newLayoutBytes));
    }
    buffer.put(newLayoutBytes);
    Files.write(path, buffer.array(), StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.SYNC);

    String mgmtFileName = dsExtension ? "MANAGEMENT_LAYOUT.ds" : "MANAGEMENT_LAYOUT";
    Path mgmtPath = Paths.get(path.getParent().toString(), mgmtFileName);

    Files.write(mgmtPath, buffer.array(), StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.SYNC);

    System.out.println("Generated new layout in " + path.getFileName() + " " + newJsonPayload);
    System.out.println("Updated " + path.toString() + " and " + mgmtPath.toString());
  }

  private static int getChecksum(byte[] bytes) {
    Hasher hasher = Hashing.crc32c().newHasher();
    for (byte a : bytes) {
      hasher.putByte(a);
    }

    return hasher.hash().asInt();
  }
}
