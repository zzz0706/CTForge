package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestSlowLookupThresholdMsConfig {

  private Configuration conf;

  @Before
  public void setUp() {
    conf = new Configuration();
  }

  @After
  public void tearDown() {
    conf = null;
  }

  @Test
  public void testSlowLookupThresholdMsValid() {
    // 1. Obtain configuration value from the loaded configuration files (no hard-coding)
    int threshold = conf.getInt(
        CommonConfigurationKeys.HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_KEY,
        CommonConfigurationKeys.HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_DEFAULT);

    // 2. Constraint check: must be a positive integer
    assertTrue("hadoop.security.dns.log-slow-lookups.threshold.ms must be a positive integer",
        threshold > 0);

    // 3. Dependency check: only meaningful when slow lookup logging is enabled
    boolean enabled = conf.getBoolean(
        "hadoop.security.dns.log-slow-lookups.enabled",
        false);

    if (enabled) {
      assertTrue("hadoop.security.dns.log-slow-lookups.threshold.ms must be > 0 when slow lookup logging is enabled",
          threshold > 0);
    }
  }
}