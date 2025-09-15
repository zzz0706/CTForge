package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class TestSlowLookupThresholdMsConfig {

  @Test
  public void testSlowLookupThresholdMsValid() {
    Configuration conf = new Configuration();
    boolean logSlowLookupsEnabled = conf.getBoolean(
        "hadoop.security.dns.log-slow-lookups.enabled", false);
    int thresholdMs = conf.getInt(
        CommonConfigurationKeys.HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_KEY,
        CommonConfigurationKeys.HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_DEFAULT);

    // The threshold must be a non-negative integer
    assertTrue("hadoop.security.dns.log-slow-lookups.threshold.ms must be >= 0",
        thresholdMs >= 0);

    // If slow lookup logging is disabled, any non-negative threshold is acceptable
    if (logSlowLookupsEnabled) {
      // When logging is enabled, a threshold of 0 is allowed but effectively logs every lookup
      assertTrue("hadoop.security.dns.log-slow-lookups.threshold.ms must be >= 0 when logging enabled",
          thresholdMs >= 0);
    }
  }
}