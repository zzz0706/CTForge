package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestFileStreamBufferSizeConfig {

  @Test
  public void testFileStreamBufferSizeConstraints() {
    Configuration conf = new Configuration();
    conf.addResource("core-default.xml");

    int streamBufferSize = conf.getInt("file.stream-buffer-size", 4096);
    int bytesPerChecksum = conf.getInt("file.bytes-per-checksum", 512);

    // Rule: bytes-per-checksum must not be larger than stream-buffer-size
    assertTrue("file.bytes-per-checksum (" + bytesPerChecksum +
               ") must not be larger than file.stream-buffer-size (" +
               streamBufferSize + ")",
               bytesPerChecksum <= streamBufferSize);

    // Rule: stream-buffer-size should be a multiple of hardware page size (4096)
    assertEquals("file.stream-buffer-size should be a multiple of 4096",
                 0, streamBufferSize % 4096);
  }
}