package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class TestFileStreamBufferSizeConfig {

  @Test
  public void testFileStreamBufferSizeConstraints() {
    Configuration conf = new Configuration();
    // Do NOT set any values programmatically; read from the loaded configuration files

    int streamBufferSize = conf.getInt(
        LocalFileSystemConfigKeys.LOCAL_FS_STREAM_BUFFER_SIZE_KEY,
        LocalFileSystemConfigKeys.LOCAL_FS_STREAM_BUFFER_SIZE_DEFAULT);

    int bytesPerChecksum = conf.getInt("file.bytes-per-checksum", 512);

    // Constraint 1: must be a positive integer
    assertTrue("file.stream-buffer-size must be positive",
               streamBufferSize > 0);

    // Constraint 2: must be a multiple of hardware page size (4096 on Intel x86)
    assertTrue("file.stream-buffer-size should be a multiple of 4096",
               streamBufferSize % 4096 == 0);

    // Dependency: bytesPerChecksum must not be larger than streamBufferSize
    assertTrue("file.bytes-per-checksum must not be larger than file.stream-buffer-size",
               bytesPerChecksum <= streamBufferSize);
  }
}