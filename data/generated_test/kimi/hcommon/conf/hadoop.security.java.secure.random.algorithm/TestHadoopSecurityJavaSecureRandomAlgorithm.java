package org.apache.hadoop.crypto;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SecureRandom;
import java.security.Security;

import static org.junit.Assert.*;

public class TestHadoopSecurityJavaSecureRandomAlgorithm {

  private Configuration conf;
  private JceAesCtrCryptoCodec codec;

  @Before
  public void setUp() {
    conf = new Configuration();
    codec = new JceAesCtrCryptoCodec();
  }

  @After
  public void tearDown() {
    conf.clear();
    codec = null;
  }

  @Test
  public void testValidAlgorithm() {
    // 1. Obtain configuration value (not set in test code)
    String algorithm = conf.get(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_JAVA_SECURE_RANDOM_ALGORITHM_KEY,
        CommonConfigurationKeysPublic.HADOOP_SECURITY_JAVA_SECURE_RANDOM_ALGORITHM_DEFAULT);

    // 2. Verify algorithm is available in JVM
    try {
      SecureRandom.getInstance(algorithm);
    } catch (NoSuchAlgorithmException e) {
      fail("Configured algorithm '" + algorithm + "' is not available in this JVM");
    }
  }

  @Test
  public void testInvalidAlgorithm() {
    // Simulate invalid algorithm by setting it explicitly (only for validation test)
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_JAVA_SECURE_RANDOM_ALGORITHM_KEY, "INVALID_ALGORITHM");

    // 1. Obtain configuration value
    String algorithm = conf.get(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_JAVA_SECURE_RANDOM_ALGORITHM_KEY,
        CommonConfigurationKeysPublic.HADOOP_SECURITY_JAVA_SECURE_RANDOM_ALGORITHM_DEFAULT);

    // 2. Verify algorithm is invalid
    try {
      SecureRandom.getInstance(algorithm);
      fail("Expected NoSuchAlgorithmException for invalid algorithm");
    } catch (NoSuchAlgorithmException expected) {
      // Expected
    }
  }

  @Test
  public void testAlgorithmWithProvider() {
    // 1. Obtain configuration value
    String algorithm = conf.get(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_JAVA_SECURE_RANDOM_ALGORITHM_KEY,
        CommonConfigurationKeysPublic.HADOOP_SECURITY_JAVA_SECURE_RANDOM_ALGORITHM_DEFAULT);

    // 2. Verify algorithm is available with any provider
    boolean found = false;
    for (java.security.Provider provider : Security.getProviders()) {
      try {
        SecureRandom.getInstance(algorithm, provider.getName());
        found = true;
        break;
      } catch (NoSuchAlgorithmException | NoSuchProviderException e) {
        // Continue checking other providers
      }
    }
    assertTrue("Algorithm '" + algorithm + "' is not available with any provider", found);
  }

  @Test
  public void testCodecInitializationWithConfiguredAlgorithm() {
    // 1. Obtain configuration value
    String algorithm = conf.get(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_JAVA_SECURE_RANDOM_ALGORITHM_KEY,
        CommonConfigurationKeysPublic.HADOOP_SECURITY_JAVA_SECURE_RANDOM_ALGORITHM_DEFAULT);

    // 2. Test codec initialization
    codec.setConf(conf);
    
    // 3. Verify codec can generate random bytes
    byte[] bytes = new byte[16];
    codec.generateSecureRandom(bytes);
    assertNotNull(bytes);
    assertEquals(16, bytes.length);
  }
}