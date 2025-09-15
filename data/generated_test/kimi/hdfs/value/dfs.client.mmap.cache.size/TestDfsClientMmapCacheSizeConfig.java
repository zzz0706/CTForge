package org.apache.hadoop.hdfs.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.Mmap;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class TestDfsClientMmapCacheSizeConfig {

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
   * Test that the configuration value for dfs.client.mmap.cache.size
   * is a non-negative integer.
   */
  @Test
  public void testMmapCacheSizeNonNegative() {
    int cacheSize = conf.getInt(Mmap.CACHE_SIZE_KEY, Mmap.CACHE_SIZE_DEFAULT);
    assertTrue("dfs.client.mmap.cache.size must be non-negative",
        cacheSize >= 0);
  }

  /**
   * Test that the configuration value for dfs.client.mmap.cache.size
   * is a valid integer (not a float or other type).
   */
  @Test
  public void testMmapCacheSizeIsInteger() {
    String rawValue = conf.getRaw(Mmap.CACHE_SIZE_KEY);
    if (rawValue != null) {
      try {
        Integer.parseInt(rawValue);
      } catch (NumberFormatException e) {
        assertTrue("dfs.client.mmap.cache.size must be a valid integer", false);
      }
    }
  }
}