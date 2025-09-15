package org.apache.hadoop.crypto;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SecureRandom;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.Test;

public class TestSecureRandomAlgorithmConfig {

  @Test
  public void testHadoopSecurityJavaSecureRandomAlgorithm() {
    Configuration conf = new Configuration();
    String key = CommonConfigurationKeysPublic.HADOOP_SECURITY_JAVA_SECURE_RANDOM_ALGORITHM_KEY;
    String value = conf.get(key, CommonConfigurationKeysPublic.HADOOP_SECURITY_JAVA_SECURE_RANDOM_ALGORITHM_DEFAULT);

    // 1. The value must be a non-empty string
    assertTrue("Value for " + key + " must not be empty", value != null && !value.trim().isEmpty());

    // 2. The value must be a valid Java SecureRandom algorithm
    try {
      // Try to instantiate SecureRandom with the given algorithm
      SecureRandom.getInstance(value);
    } catch (NoSuchAlgorithmException e) {
      fail("Invalid SecureRandom algorithm: " + value + " for key " + key);
    }

    // 3. If a JCE provider is specified, ensure the algorithm is supported by that provider
    String provider = conf.get(CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_JCE_PROVIDER_KEY);
    if (provider != null && !provider.trim().isEmpty()) {
      try {
        SecureRandom.getInstance(value, provider);
      } catch (NoSuchAlgorithmException | NoSuchProviderException e) {
        fail("SecureRandom algorithm " + value + " is not supported by provider " + provider);
      }
    }
  }
}