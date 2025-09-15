package org.apache.hadoop.conf;

import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestIoBytesPerChecksumConfig {

  @Test
  public void testIoBytesPerChecksumValidRange() throws Exception {
    Configuration conf = new Configuration(false);
    conf.addResource("core-site.xml");

    int bytesPerChecksum = conf.getInt("io.bytes.per.checksum", 512);
    int fileBufferSize   = conf.getInt("io.file.buffer.size", 4096);

    // Rule: io.bytes.per.checksum must not be larger than io.file.buffer.size
    assertTrue("io.bytes.per.checksum (" + bytesPerChecksum +
               ") must not be larger than io.file.buffer.size (" +
               fileBufferSize + ")",
               bytesPerChecksum <= fileBufferSize);

    // Rule: value must be a positive integer
    assertTrue("io.bytes.per.checksum must be > 0", bytesPerChecksum > 0);
  }

  @Test
  public void testFsServerDefaultsUsesValidBytesPerChecksum() throws Exception {
    Configuration conf = new Configuration(false);
    conf.addResource("core-site.xml");

    // Let FileSystem build its defaults from the same configuration
    FileSystem fs = FileSystem.getLocal(conf);
    FsServerDefaults defaults = fs.getServerDefaults();

    int bytesPerChecksum = defaults.getBytesPerChecksum();
    int fileBufferSize   = conf.getInt("io.file.buffer.size", 4096);

    assertTrue("FsServerDefaults.bytesPerChecksum (" + bytesPerChecksum +
               ") must not be larger than io.file.buffer.size (" +
               fileBufferSize + ")",
               bytesPerChecksum <= fileBufferSize);

    assertTrue("FsServerDefaults.bytesPerChecksum must be > 0",
               bytesPerChecksum > 0);
  }
}