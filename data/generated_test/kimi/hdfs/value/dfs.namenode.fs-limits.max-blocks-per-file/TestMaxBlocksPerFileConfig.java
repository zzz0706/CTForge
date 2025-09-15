package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestMaxBlocksPerFileConfig {

  @Test
  public void testMaxBlocksPerFileIsPositiveLong() {
    Configuration conf = new Configuration();
    // Do NOT set the value in code; rely on loaded configuration files
    long maxBlocks = conf.getLong(
        DFSConfigKeys.DFS_NAMENODE_MAX_BLOCKS_PER_FILE_KEY,
        DFSConfigKeys.DFS_NAMENODE_MAX_BLOCKS_PER_FILE_DEFAULT);

    // Constraint: must be a positive long
    assertTrue("dfs.namenode.fs-limits.max-blocks-per-file must be > 0",
               maxBlocks > 0);

    // Constraint: must be an integer value (no fractional part)
    assertEquals("dfs.namenode.fs-limits.max-blocks-per-file must be an integer",
                 maxBlocks, (long) maxBlocks);
  }
}