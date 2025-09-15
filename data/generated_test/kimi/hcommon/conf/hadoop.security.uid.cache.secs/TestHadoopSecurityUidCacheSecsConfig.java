package org.apache.hadoop.io.nativeio;

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.Test;

public class TestHadoopSecurityUidCacheSecsConfig {

  @Test
  public void testHadoopSecurityUidCacheSecsValid() {
    // 1. Obtain the configuration value using the hadoop-common2.8.5 API
    Configuration conf = new Configuration();
    long cacheSecs = conf.getLong(
        CommonConfigurationKeys.HADOOP_SECURITY_UID_NAME_CACHE_TIMEOUT_KEY,
        CommonConfigurationKeys.HADOOP_SECURITY_UID_NAME_CACHE_TIMEOUT_DEFAULT);

    // 2. Prepare the test conditions: none needed beyond reading the value

    // 3. Test code: verify the value is a positive integer
    assertTrue("hadoop.security.uid.cache.secs must be a positive integer",
               cacheSecs > 0);

    // 4. Code after testing: none
  }
}