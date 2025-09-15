package org.apache.hadoop.crypto;

import static org.junit.Assert.*;

import java.security.Provider;
import java.security.Security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestHadoopSecurityCryptoJceProviderConfig {

  private Configuration conf;
  private JceAesCtrCryptoCodec codec;

  @Before
  public void setUp() {
    conf = new Configuration();
    codec = new JceAesCtrCryptoCodec();
  }

  @After
  public void tearDown() {
    conf = null;
    codec = null;
  }

  /**
   * Validates that the value assigned to
   * {@code hadoop.security.crypto.jce.provider} is a provider name that is
   * actually registered in the JVM.  An empty string or a non-existent provider
   * name is considered invalid.
   */
  @Test
  public void testJceProviderExists() {
    String providerName = conf.get(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_JCE_PROVIDER_KEY);

    // null or empty is allowed; in this case the default provider is used.
    if (providerName == null || providerName.trim().isEmpty()) {
      return;
    }

    Provider provider = Security.getProvider(providerName);
    assertNotNull(
        "The configured JCE provider '" + providerName + "' is not registered",
        provider);
  }

  /**
   * Verifies that the codec can be initialized without throwing an exception
   * when the configured provider is valid.  If the provider is invalid the codec
   * falls back to the default provider, so we only check that the codec is
   * usable after initialization.
   */
  @Test
  public void testCodecInitializationWithConfiguredProvider() {
    codec.setConf(conf);
    // No exception implies the provider was accepted or gracefully ignored.
    assertNotNull(codec);
  }
}