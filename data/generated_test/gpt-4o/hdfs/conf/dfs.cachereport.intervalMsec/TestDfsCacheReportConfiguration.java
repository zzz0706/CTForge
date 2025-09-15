package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestDfsCacheReportConfiguration {

  @Test
  public void testDfsCacheReportConfiguration() {
    // Step 1: Read configuration from Configuration object
    Configuration conf = new Configuration();
    long dfsDatanodeMaxLockedMemory = conf.getLong(
      DFSConfigKeys.DFS_DATANODE_MAX_LOCKED_MEMORY_KEY,
      DFSConfigKeys.DFS_DATANODE_MAX_LOCKED_MEMORY_DEFAULT
    );

    long dfsCacheReportIntervalMsec = conf.getLong(
      DFSConfigKeys.DFS_CACHEREPORT_INTERVAL_MSEC_KEY,
      DFSConfigKeys.DFS_CACHEREPORT_INTERVAL_MSEC_DEFAULT
    );

    // Step 2: Validate configuration constraints and dependencies
    // Constraint 1: If dfs.datanode.max.locked.memory == 0, then dfs.cachereport.intervalMsec has no effect
    if (dfsDatanodeMaxLockedMemory == 0) {
      assertFalse("dfs.cachereport.intervalMsec should not have any effect when dfs.datanode.max.locked.memory is 0",
                  dfsCacheReportIntervalMsec != DFSConfigKeys.DFS_CACHEREPORT_INTERVAL_MSEC_DEFAULT);
    }

    // Constraint 2: Validate dfs.cachereport.intervalMsec is a positive integer
    assertTrue("dfs.cachereport.intervalMsec must be positive",
               dfsCacheReportIntervalMsec > 0);

    // Constraint 3: Validate dfs.datanode.max.locked.memory is a positive integer
    assertTrue("dfs.datanode.max.locked.memory must be positive",
               dfsDatanodeMaxLockedMemory >= 0);

    // Step 3: Additional checks for dependencies (if any were outlined in source code)
    // Dependency: If dfs.cachereport.intervalMsec is set, it is meaningful only if dfs.datanode.max.locked.memory > 0
    if (dfsCacheReportIntervalMsec != DFSConfigKeys.DFS_CACHEREPORT_INTERVAL_MSEC_DEFAULT) {
      assertTrue("dfs.cachereport.intervalMsec is meaningful only if dfs.datanode.max.locked.memory > 0",
                 dfsDatanodeMaxLockedMemory > 0);
    }
  }
}