package org.apache.hadoop.crypto;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class CryptoCodecTest {

  @Test
  public void testDefaultCipherSuiteIsLoaded() {
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    Configuration conf = new Configuration();

    // 2. Prepare the test conditions.
    // The default configuration already contains the necessary settings for CryptoCodec.

    // 3. Test code.
    CryptoCodec actualCodec = CryptoCodec.getInstance(conf);

    // 4. Code after testing.
    assertNotNull("CryptoCodec should not be null", actualCodec);
    assertEquals("Cipher suite name mismatch", CipherSuite.AES_CTR_NOPADDING.getName(),
            actualCodec.getCipherSuite().getName());
  }
}