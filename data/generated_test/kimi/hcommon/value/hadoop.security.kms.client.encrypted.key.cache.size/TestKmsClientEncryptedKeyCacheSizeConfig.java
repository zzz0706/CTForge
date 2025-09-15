package org.apache.hadoop.crypto.key.kms;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestKmsClientEncryptedKeyCacheSizeConfig {

  private Configuration conf;

  @Before
  public void setUp() {
    conf = new Configuration();
  }

  @After
  public void tearDown() {
    conf.clear();
  }

  /**
   * Tests that the configuration value for
   * hadoop.security.kms.client.encrypted.key.cache.size
   * must be a positive integer.
   * KMSClientProvider passes it directly to ValueQueue which
   * asserts numValues > 0 in its constructor.
   */
  @Test
  public void testEncryptedKeyCacheSizePositive() throws IOException, URISyntaxException {
    // 1. Load configuration from classpath (hdfs-site.xml, core-site.xml, etc.)
    //    No explicit setting in code to respect external files.
    int cacheSize = conf.getInt(
        CommonConfigurationKeysPublic.KMS_CLIENT_ENC_KEY_CACHE_SIZE,
        CommonConfigurationKeysPublic.KMS_CLIENT_ENC_KEY_CACHE_SIZE_DEFAULT);

    // 2. Test the constraint: must be > 0
    assertTrue(
        "hadoop.security.kms.client.encrypted.key.cache.size must be > 0",
        cacheSize > 0);

    // 3. Ensure KMSClientProvider instantiation succeeds with the value
    new KMSClientProvider(new URI("kms://http@localhost:9600/kms"), conf);
  }
}