package org.apache.hadoop.hdfs.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestSlowIoWarningThresholdConfig {

  @Test
  public void testSlowIoWarningThresholdMsValid() {
    // 1. Obtain configuration values via the hdfs 2.8.5 API
    Configuration conf = new Configuration();

    // 2. Prepare test conditions: rely on external configuration files only
    long threshold = conf.getLong(
        HdfsClientConfigKeys.DFS_CLIENT_SLOW_IO_WARNING_THRESHOLD_KEY,
        HdfsClientConfigKeys.DFS_CLIENT_SLOW_IO_WARNING_THRESHOLD_DEFAULT);

    // 3. Test code: verify the value is a non-negative long
    assertTrue("slow io warning threshold must be non-negative",
               threshold >= 0);

    // 4. Code after testing: nothing to clean up
  }
}