package org.apache.hadoop.crypto;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class TestCryptoBufferSizeConfigValidation {

  private Configuration conf;

  @Before
  public void setUp() {
    conf = new Configuration();
  }

  @After
  public void tearDown() {
    conf = null;
  }

  /**
   * Validates that the configured value for hadoop.security.crypto.buffer.size
   * is a positive integer and not zero or negative.
   *
   * CryptoStreamUtils#getBufferSize(Configuration) uses
   * Configuration#getInt(String, int) which returns an int. The buffer is
   * subsequently used to allocate direct ByteBuffers and byte arrays, so a
   * non-positive value is invalid.
   */
  @Test
  public void testCryptoBufferSizeIsPositiveInteger() {
    int bufferSize = conf.getInt(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_BUFFER_SIZE_KEY,
        CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_BUFFER_SIZE_DEFAULT);

    assertTrue("hadoop.security.crypto.buffer.size must be > 0", bufferSize > 0);
  }
}