package org.apache.hadoop.conf;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsServerDefaults;
import org.junit.Test;

public class TestIoBytesPerChecksumConfig {

  @Test
  public void testIoBytesPerChecksumNotLargerThanIoFileBufferSize() {
    Configuration conf = new Configuration();
    
    int bytesPerChecksum = conf.getInt("io.bytes.per.checksum", 512);
    int fileBufferSize = conf.getInt("io.file.buffer.size", 4096);
    
    assertTrue("io.bytes.per.checksum (" + bytesPerChecksum + 
               ") must not be larger than io.file.buffer.size (" + fileBufferSize + ")",
               bytesPerChecksum <= fileBufferSize);
  }

  @Test
  public void testIoBytesPerChecksumPositive() {
    Configuration conf = new Configuration();
    
    int bytesPerChecksum = conf.getInt("io.bytes.per.checksum", 512);
    
    assertTrue("io.bytes.per.checksum must be positive", bytesPerChecksum > 0);
  }

  @Test
  public void testFsServerDefaultsRespectsIoBytesPerChecksum() {
    Configuration conf = new Configuration();
    
    try {
      FileSystem fs = FileSystem.getLocal(conf);
      FsServerDefaults defaults = fs.getServerDefaults();
      
      int expected = conf.getInt("io.bytes.per.checksum", 512);
      int actual = defaults.getBytesPerChecksum();
      
      assertEquals("FsServerDefaults should reflect io.bytes.per.checksum", expected, actual);
    } catch (IOException e) {
      fail("Unexpected exception while retrieving FsServerDefaults: " + e.getMessage());
    }
  }
}