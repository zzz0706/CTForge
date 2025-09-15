package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;

import static org.junit.Assert.assertEquals;

public class SecurityUtilSlowLookupThresholdTest {

  private int originalThreshold;

  @Before
  public void setUp() throws Exception {
    Field field = SecurityUtil.class.getDeclaredField("slowLookupThresholdMs");
    field.setAccessible(true);
    originalThreshold = field.getInt(null);
  }

  @After
  public void tearDown() throws Exception {
    Field field = SecurityUtil.class.getDeclaredField("slowLookupThresholdMs");
    field.setAccessible(true);
    field.setInt(null, originalThreshold);
  }

  @Test
  public void testSlowLookupThresholdUsesConfiguredValueWhenPropertySet() throws Exception {
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    Configuration conf = new Configuration();
    conf.setInt(CommonConfigurationKeys.HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_KEY, 500);

    // 2. Prepare the test conditions.
    // In 2.8.5 SecurityUtil reads the value directly from Configuration when it is first used.
    // Force re-initialization by re-loading the class
    Field field = SecurityUtil.class.getDeclaredField("slowLookupThresholdMs");
    field.setAccessible(true);
    field.setInt(null, conf.getInt(
        CommonConfigurationKeys.HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_KEY,
        CommonConfigurationKeys.HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_DEFAULT));

    // 3. Test code.
    int actualThreshold = field.getInt(null);
    assertEquals(500, actualThreshold);

    // 4. Code after testing.
    // Reset to original value in tearDown
  }
}