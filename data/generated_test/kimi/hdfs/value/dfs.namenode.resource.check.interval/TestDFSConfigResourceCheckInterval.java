package org.apache.hadoop.hdfs.server.namenode;

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestDFSConfigResourceCheckInterval {

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
  public void testResourceCheckIntervalPositive() {
    long interval = conf.getLong(
        DFSConfigKeys.DFS_NAMENODE_RESOURCE_CHECK_INTERVAL_KEY,
        DFSConfigKeys.DFS_NAMENODE_RESOURCE_CHECK_INTERVAL_DEFAULT);

    // The interval must be a positive integer (milliseconds)
    assertTrue("dfs.namenode.resource.check.interval must be > 0",
               interval > 0);
  }
}