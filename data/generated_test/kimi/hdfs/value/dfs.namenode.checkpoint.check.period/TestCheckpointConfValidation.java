package org.apache.hadoop.hdfs.server.namenode;

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Test;

public class TestCheckpointConfValidation {

  @Test
  public void testCheckpointCheckPeriodSmallerThanCheckpointPeriod() {
    // 1. Use the hdfs 2.8.5 API to obtain configuration values
    Configuration conf = new Configuration();
    
    // 2. Prepare the test conditions
    long checkpointCheckPeriod = conf.getLong(
        DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_CHECK_PERIOD_KEY,
        DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_CHECK_PERIOD_DEFAULT);
    
    long checkpointPeriod = conf.getLong(
        DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_PERIOD_KEY,
        DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_PERIOD_DEFAULT);
    
    // 3. Test code
    assertTrue("dfs.namenode.checkpoint.check.period must be <= dfs.namenode.checkpoint.period",
        checkpointCheckPeriod <= checkpointPeriod);
    
    // 4. Code after testing (nothing to clean up)
  }
}