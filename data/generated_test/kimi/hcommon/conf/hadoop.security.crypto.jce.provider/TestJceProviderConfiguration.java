package org.apache.hadoop.crypto;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_JCE_PROVIDER_KEY;

import java.security.Provider;
import java.security.Security;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestJceProviderConfiguration {

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
   * Validates that the configured JCE provider name is either
   * 1) empty/unset (falls back to default provider selection) or
   * 2) matches a currently registered JCE provider.
   */
  @Test
  public void testValidJceProvider() {
    String providerName = conf.get(HADOOP_SECURITY_CRYPTO_JCE_PROVIDER_KEY);

    // Empty or unset is always valid
    if (providerName == null || providerName.trim().isEmpty()) {
      return;
    }

    // Otherwise the provider must be registered in the JVM
    Provider[] providers = Security.getProviders();
    boolean found = false;
    for (Provider p : providers) {
      if (providerName.equals(p.getName())) {
        found = true;
        break;
      }
    }
    Assert.assertTrue(
        "Configured JCE provider '" + providerName + "' is not registered in the JVM",
        found);
  }
}