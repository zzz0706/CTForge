package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Assert;
import org.junit.Test;

public class TestNameNodeConfiguration {

  @Test
  public void testDFSNameNodeResourceDUReservedConfiguration() {
    // 1. Prepare the configuration object
    Configuration conf = new Configuration(); // Load configurations from the context or config file

    // 2. Obtain the configuration value using the HDFS 2.8.5 API
    long dfsNameNodeResourceDUReserved = conf.getLong(
        DFSConfigKeys.DFS_NAMENODE_DU_RESERVED_KEY,
        DFSConfigKeys.DFS_NAMENODE_DU_RESERVED_DEFAULT);

    // 3. Validate the retrieved configuration value
    // Ensure it is a non-negative value (as reserved disk space cannot be negative)
    Assert.assertTrue(
        "Configuration 'dfs.namenode.resource.du.reserved' should be non-negative",
        dfsNameNodeResourceDUReserved >= 0);

    // NOTE: If new constraints are added or dependencies with other configurations exist,
    // make sure to validate them here in the future.
  }
}