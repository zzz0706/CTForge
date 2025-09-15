package org.apache.hadoop.hdfs.server.namenode;

import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestEditLogAutoRollMultiplierThreshold {

  private Configuration conf;

  @Before
  public void setUp() {
    // 1. Obtain configuration values via the hdfs 2.8.5 API
    conf = new Configuration();
  }

  @After
  public void tearDown() {
    // 4. Code after testing
    conf.clear();
  }

  @Test
  public void testAutorollMultiplierThresholdValid() {
    // 2. Prepare test conditions: read values from configuration files
    float multiplier = conf.getFloat(
        DFSConfigKeys.DFS_NAMENODE_EDIT_LOG_AUTOROLL_MULTIPLIER_THRESHOLD,
        DFSConfigKeys.DFS_NAMENODE_EDIT_LOG_AUTOROLL_MULTIPLIER_THRESHOLD_DEFAULT);

    long checkpointTxns = conf.getLong(
        DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_TXNS_KEY,
        DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_TXNS_DEFAULT);

    // 3. Test code: validate constraints and dependencies
    // multiplier must be a positive float
    assertTrue("dfs.namenode.edit.log.autoroll.multiplier.threshold must be positive",
               multiplier > 0.0f);

    // checkpointTxns must be a positive long
    assertTrue("dfs.namenode.checkpoint.txns must be positive",
               checkpointTxns > 0L);

    // The computed threshold must not overflow long
    long computed = (long) (multiplier * checkpointTxns);
    assertTrue("Computed editLogRollerThreshold must be non-negative",
               computed >= 0L);
  }
}