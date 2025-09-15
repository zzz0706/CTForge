package org.apache.hadoop.crypto.key.kms;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestKMSClientEncryptedKeyCacheExpiryConfig {

  private Configuration conf;

  @Before
  public void setUp() {
    // 1. Obtain configuration values via the hdfs 2.8.5 API
    conf = new Configuration();
  }

  @After
  public void tearDown() {
    conf = null;
  }

  @Test
  public void testEncryptedKeyCacheExpiryValid() {
    // 2. Prepare the test conditions – rely on default or external config
    long expiry = conf.getLong(
        CommonConfigurationKeysPublic.KMS_CLIENT_ENC_KEY_CACHE_EXPIRY_MS,
        CommonConfigurationKeysPublic.KMS_CLIENT_ENC_KEY_CACHE_EXPIRY_DEFAULT);

    // 3. Test code – verify the constraint: expiry must be > 0
    assertTrue(
        "hadoop.security.kms.client.encrypted.key.cache.expiry must be > 0",
        expiry > 0);
  }
}