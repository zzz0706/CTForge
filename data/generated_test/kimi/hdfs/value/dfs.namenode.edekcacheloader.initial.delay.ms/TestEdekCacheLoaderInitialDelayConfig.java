package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class TestEdekCacheLoaderInitialDelayConfig {

  private Configuration conf;

  @Before
  public void setUp() {
    // 1. Use the hdfs 2.8.5 API to obtain configuration values
    conf = new Configuration();
  }

  @After
  public void tearDown() {
    conf = null;
  }

  @Test
  public void testEdekCacheLoaderInitialDelayMsValid() {
    // 2. Prepare the test conditions – read the value from the configuration file
    int delay = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_EDEKCACHELOADER_INITIAL_DELAY_MS_KEY,
        DFSConfigKeys.DFS_NAMENODE_EDEKCACHELOADER_INITIAL_DELAY_MS_DEFAULT);

    // 3. Test code – validate the constraints
    // According to the source code, this value is used as the initial delay in milliseconds
    // for the EDEK cache warming thread. It must be a non-negative integer.
    assertTrue("dfs.namenode.edekcacheloader.initial.delay.ms must be non-negative",
               delay >= 0);
  }
}