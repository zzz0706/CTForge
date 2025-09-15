package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestCacheReportIntervalConfig {

  @Test
  public void testCacheReportIntervalWhenCachingDisabled() {
    // 1. Obtain configuration values via the HDFS 2.8.5 API
    Configuration conf = new Configuration();

    // 2. Prepare the test conditions: set dfs.datanode.max.locked.memory to 0 (disabled)
    conf.setLong(DFSConfigKeys.DFS_DATANODE_MAX_LOCKED_MEMORY_KEY, 0);

    // 3. Test code: create DNConf and verify that cacheReportInterval is read but has no effect
    DNConf dnConf = new DNConf(conf);
    long cacheReportInterval = dnConf.cacheReportInterval;
    assertTrue("cacheReportInterval should be read from config even when caching is disabled",
               cacheReportInterval > 0);

    // 4. Code after testing: no special cleanup required
  }

  @Test
  public void testCacheReportIntervalWhenCachingEnabled() {
    // 1. Obtain configuration values via the HDFS 2.8.5 API
    Configuration conf = new Configuration();

    // 2. Prepare the test conditions: set dfs.datanode.max.locked.memory > 0
    conf.setLong(DFSConfigKeys.DFS_DATANODE_MAX_LOCKED_MEMORY_KEY, 1024 * 1024);

    // 3. Test code: create DNConf and verify that cacheReportInterval is read and positive
    DNConf dnConf = new DNConf(conf);
    long cacheReportInterval = dnConf.cacheReportInterval;
    assertTrue("cacheReportInterval must be positive when caching is enabled",
               cacheReportInterval > 0);

    // 4. Code after testing: no special cleanup required
  }

  @Test
  public void testCacheReportIntervalDefaultValue() {
    // 1. Obtain configuration values via the HDFS 2.8.5 API
    Configuration conf = new Configuration();

    // 2. Prepare the test conditions: do not set dfs.cachereport.intervalMsec explicitly
    // 3. Test code: create DNConf and verify the default value is 10000
    DNConf dnConf = new DNConf(conf);
    long cacheReportInterval = dnConf.cacheReportInterval;
    assertEquals("Default value of dfs.cachereport.intervalMsec should be 10000",
                 10000L, cacheReportInterval);

    // 4. Code after testing: no special cleanup required
  }

  @Test
  public void testCacheReportIntervalNegativeValue() {
    // 1. Obtain configuration values via the HDFS 2.8.5 API
    Configuration conf = new Configuration();

    // 2. Prepare the test conditions: set dfs.cachereport.intervalMsec to a negative value
    conf.setLong(DFSConfigKeys.DFS_CACHEREPORT_INTERVAL_MSEC_KEY, -1);

    // 3. Test code: create DNConf and verify the negative value is accepted (no explicit range check)
    DNConf dnConf = new DNConf(conf);
    long cacheReportInterval = dnConf.cacheReportInterval;
    assertEquals("Negative value for dfs.cachereport.intervalMsec should be accepted",
                 -1L, cacheReportInterval);

    // 4. Code after testing: no special cleanup required
  }
}