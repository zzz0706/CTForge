package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestDecommissionBlocksPerIntervalConfig {

  private Configuration conf;

  @Before
  public void setUp() {
    conf = new Configuration();
  }

  @After
  public void tearDown() {
    conf.clear();
  }

  /**
   * Test that the configuration key exists and is a positive integer.
   * The value must be greater than 0.
   */
  @Test
  public void testBlocksPerIntervalPositive() {
    int blocksPerInterval = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_KEY,
        DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_DEFAULT);

    assertTrue("dfs.namenode.decommission.blocks.per.interval must be > 0",
               blocksPerInterval > 0);
  }

  /**
   * Test that the configuration key is not zero.
   */
  @Test
  public void testBlocksPerIntervalNotZero() {
    int blocksPerInterval = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_KEY,
        DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_DEFAULT);

    assertNotEquals("dfs.namenode.decommission.blocks.per.interval cannot be 0",
                    0, blocksPerInterval);
  }

  /**
   * Test that the configuration key is not negative.
   */
  @Test
  public void testBlocksPerIntervalNotNegative() {
    int blocksPerInterval = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_KEY,
        DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_DEFAULT);

    assertFalse("dfs.namenode.decommission.blocks.per.interval must not be negative",
                blocksPerInterval < 0);
  }
}