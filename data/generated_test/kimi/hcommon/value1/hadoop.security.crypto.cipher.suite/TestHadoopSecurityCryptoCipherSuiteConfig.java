package org.apache.hadoop.crypto;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.Test;

public class TestHadoopSecurityCryptoCipherSuiteConfig {

  private static final List<String> ALLOWED_CIPHER_SUITES = Arrays.asList(
      "AES/CTR/NoPadding");

  @Test
  public void testCipherSuiteConfigIsValid() {
    Configuration conf = new Configuration();
    String cipherSuite = conf.get(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_CIPHER_SUITE_KEY,
        CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_CIPHER_SUITE_DEFAULT);

    assertNotNull("Cipher suite configuration must not be null", cipherSuite);
    assertTrue("Cipher suite must be one of the allowed values: " + ALLOWED_CIPHER_SUITES,
        ALLOWED_CIPHER_SUITES.contains(cipherSuite));

    // Ensure CryptoCodec can be instantiated with the configured cipher suite
    CryptoCodec codec = CryptoCodec.getInstance(conf);
    assertNotNull("CryptoCodec should be instantiated for valid cipher suite", codec);
  }
}