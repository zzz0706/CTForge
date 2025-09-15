package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestGroupsCacheSecsConfig {

  @Test
  public void testCacheSecsValid() {
    Configuration conf = new Configuration(false);
    // rely on default value
    long secs = conf.getLong(CommonConfigurationKeys.HADOOP_SECURITY_GROUPS_CACHE_SECS,
                             CommonConfigurationKeys.HADOOP_SECURITY_GROUPS_CACHE_SECS_DEFAULT);
    // must be positive
    assertTrue("hadoop.security.groups.cache.secs must be > 0", secs > 0);
  }
}