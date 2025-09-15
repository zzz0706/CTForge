package org.apache.hadoop.crypto;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestHadoopSecurityCryptoCipherSuiteConfig {

  @Test
  public void testValidCipherSuiteConfiguration() {
    // 1. Obtain the configuration value via the public API without hard-coding it.
    Configuration conf = new Configuration();
    String cipherSuite = conf.get(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_CIPHER_SUITE_KEY,
        CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_CIPHER_SUITE_DEFAULT);

    // 2. Test that the value can be converted to a CipherSuite enum.
    CipherSuite suite = CipherSuite.convert(cipherSuite);
    assertNotNull(
        "Configured cipher suite '" + cipherSuite + "' is not a valid CipherSuite",
        suite);
  }

  @Test
  public void testCryptoCodecCanBeInstantiated() {
    // 1. Obtain the configuration value via the public API.
    Configuration conf = new Configuration();
    String cipherSuite = conf.get(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_CIPHER_SUITE_KEY,
        CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_CIPHER_SUITE_DEFAULT);

    // 2. Ensure the configured cipher suite can be used to obtain a CryptoCodec.
    CryptoCodec codec = CryptoCodec.getInstance(conf);
    assertNotNull(
        "No CryptoCodec implementation found for cipher suite '" + cipherSuite + "'",
        codec);
  }
}