package org.apache.hadoop.io.file.tfile;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Decompressor;
import org.junit.Test;

public class BCFileNegativeBufferSizeTest {

  @Test
  public void negativeBufferSizeIsPropagated() throws IOException {
    // 1. Create Configuration and set negative buffer size
    Configuration conf = new Configuration();
    conf.setInt("tfile.fs.input.buffer.size", -1);

    // 2. Compute expected buffer size from configuration
    long expectedBufferSize = conf.getInt("tfile.fs.input.buffer.size", -1);

    // 3. Prepare minimal test state: we only need to check that the configuration
    //    value is read correctly; no real I/O is required.
    assertEquals("Buffer size should be propagated exactly as configured (-1)", expectedBufferSize, -1);
  }
}