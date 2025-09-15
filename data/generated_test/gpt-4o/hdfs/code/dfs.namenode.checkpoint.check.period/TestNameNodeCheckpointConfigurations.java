package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Assert;
import org.junit.Test;

public class TestNameNodeCheckpointConfigurations {

  @Test
  public void testCheckpointPeriodConfiguration() {
    Configuration conf = new Configuration();

    // Step 1: Read the configuration values from the Configuration object.
    long checkpointCheckPeriod = conf.getLong(
        DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_CHECK_PERIOD_KEY,
        DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_CHECK_PERIOD_DEFAULT);

    long checkpointPeriod = conf.getLong(
        DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_PERIOD_KEY,
        DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_PERIOD_DEFAULT);

    // Step 2: Validate the constraint dfs.namenode.checkpoint.check.period <= dfs.namenode.checkpoint.period.
    //          If the constraint is violated, handle it appropriately based on the requirements.
    Assert.assertTrue(
        "Configuration constraint violated: dfs.namenode.checkpoint.check.period (" + checkpointCheckPeriod +
            ") must be less than or equal to dfs.namenode.checkpoint.period (" + checkpointPeriod + ")",
        checkpointCheckPeriod <= checkpointPeriod
    );
  }
}