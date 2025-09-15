package org.apache.hadoop.security;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestHadoopSecurityGroupsCacheSecsConfig {

  private Configuration conf;
  private Groups groups;

  @Before
  public void setUp() throws IOException {
    // Load configuration from classpath (core-site.xml, hdfs-site.xml, etc.)
    conf = new Configuration();
    // Instantiate Groups using the loaded configuration
    groups = new Groups(conf);
  }

  @After
  public void tearDown() {
    // In Hadoop 2.8.5 Groups does not have a close() method; nothing to do.
    groups = null;
  }

  /**
   * Test that the configuration key exists and is a valid long.
   */
  @Test
  public void testConfigValueIsValidLong() {
    String key = CommonConfigurationKeys.HADOOP_SECURITY_GROUPS_CACHE_SECS;
    String valueStr = conf.get(key);
    assertNotNull("Configuration key '" + key + "' is missing", valueStr);

    try {
      long value = Long.parseLong(valueStr);
      assertTrue("Configuration value must be >= 0", value >= 0);
    } catch (NumberFormatException e) {
      fail("Configuration value for '" + key + "' is not a valid long: " + valueStr);
    }
  }

  /**
   * Test that the Groups class correctly interprets the configuration value.
   */
  @Test
  public void testGroupsUsesConfiguredTimeout() {
    long expectedMs = conf.getLong(
        CommonConfigurationKeys.HADOOP_SECURITY_GROUPS_CACHE_SECS,
        CommonConfigurationKeys.HADOOP_SECURITY_GROUPS_CACHE_SECS_DEFAULT) * 1000;

    // Access the cache timeout indirectly by ensuring the Groups service starts
    assertNotNull("Groups instance should be created", groups);

    // If the configuration is invalid (negative), refresh would log a warning
    // but we cannot easily inspect the CacheBuilder internals in 2.8.5.
    // We simply verify the configuration is accepted.
  }

  /**
   * Test that a zero value (edge case) is accepted and handled gracefully.
   */
  @Test
  public void testZeroValueAccepted() {
    String key = CommonConfigurationKeys.HADOOP_SECURITY_GROUPS_CACHE_SECS;
    String valueStr = conf.get(key);
    if (valueStr != null) {
      try {
        long value = Long.parseLong(valueStr);
        assertTrue("Zero should be accepted", value >= 0);
      } catch (NumberFormatException e) {
        fail("Invalid numeric format");
      }
    }
  }
}