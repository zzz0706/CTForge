package org.apache.hadoop.crypto;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestCryptoBufferSizeConfig {

  @Test
  public void testCryptoBufferSizeValid() {
    Configuration conf = new Configuration();
    // 1. Read the configured value (do NOT set it in code).
    int bufferSize = conf.getInt(
        "hadoop.security.crypto.buffer.size",
        8192);

    // 2. Constraints: must be a positive integer (CryptoStreamUtils.checkBufferSize
    //    rejects non-positive values).
    assertTrue("hadoop.security.crypto.buffer.size must be > 0",
               bufferSize > 0);
  }
}